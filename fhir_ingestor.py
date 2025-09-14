#!/usr/bin/env python3
"""
FHIR R4 Resource Ingestor for MongoDB
Ingests FHIR resources from files or HTTP endpoint, ensuring patientId linking
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import re
from flask import Flask, request, jsonify
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FHIRIngestor:
    """FHIR R4 Resource Ingestor for MongoDB"""

    # Supported resource types and their collections
    SUPPORTED_RESOURCES = {
        'Patient': 'patients',
        'Observation': 'observations',
        'Encounter': 'encounters',
        'Condition': 'conditions',
        'Procedure': 'procedures',
        'MedicationRequest': 'medication_requests',
        'DocumentReference': 'document_references',
        'AllergyIntolerance': 'allergies',
        'DiagnosticReport': 'diagnostic_reports'
    }

    def __init__(self, mongo_uri: str = None, db_name: str = None):
        """
        Initialize the FHIR ingestor

        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name
        """
        self.mongo_uri = mongo_uri or os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.db_name = db_name or os.getenv('DB_NAME', 'fhir_db')
        self.client = None
        self.db = None
        self.stats = {
            'processed': 0,
            'inserted': 0,
            'updated': 0,
            'errors': 0,
            'dead_letter': 0
        }

    def connect(self):
        """Connect to MongoDB"""
        try:
            logger.info(f"Connecting to MongoDB: {self._mask_uri(self.mongo_uri)}")
            self.client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=10000,
                retryWrites=True
            )
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            logger.info(f"Connected to database: {self.db_name}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def _mask_uri(self, uri: str) -> str:
        """Mask sensitive information in URI for logging"""
        if '@' in uri:
            parts = uri.split('@')
            creds = parts[0].split('//')[1]
            masked_creds = '***:***@'
            return uri.replace(creds, masked_creds)
        return uri

    def setup_indexes(self):
        """Create necessary indexes for all collections"""
        logger.info("Setting up indexes...")

        for resource_type, collection_name in self.SUPPORTED_RESOURCES.items():
            collection = self.db[collection_name]

            # Unique compound index for upsert
            collection.create_index([('resourceType', 1), ('id', 1)], unique=True)

            # Patient ID index for linking
            collection.create_index('patientId')

            # Date indexes where applicable
            date_fields = ['effectiveDateTime', 'issued', 'occurrenceDateTime',
                          'onsetDateTime', 'authoredOn', 'recordedDate']
            for field in date_fields:
                try:
                    collection.create_index(field)
                except:
                    pass  # Field might not exist for this resource type

        # Dead letter collection
        dead_collection = self.db['dead_fhir']
        dead_collection.create_index([('resourceType', 1), ('id', 1)])
        dead_collection.create_index('reason')

        logger.info("Indexes created successfully")

    def derive_patient_id(self, resource: Dict[str, Any]) -> Optional[str]:
        """
        Derive patientId from FHIR resource references

        Returns:
            patientId string or None if cannot be derived
        """
        resource_type = resource.get('resourceType')

        # Priority order for finding patient reference
        reference_paths = [
            'subject.reference',
            'patient.reference',
            'encounter.subject.reference' if resource_type == 'Encounter' else None
        ]

        for path in reference_paths:
            if path:
                ref = self._get_nested_value(resource, path)
                if ref and ref.startswith('Patient/'):
                    return ref.split('/', 1)[1]

        # Check contained resources for Patient
        contained = resource.get('contained', [])
        for contained_resource in contained:
            if contained_resource.get('resourceType') == 'Patient':
                patient_id = contained_resource.get('id')
                if patient_id:
                    return patient_id

        return None

    def _get_nested_value(self, obj: Dict, path: str) -> Any:
        """Get nested value from dict using dot notation"""
        keys = path.split('.')
        current = obj
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    def validate_resource(self, resource: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate basic FHIR constraints

        Returns:
            (is_valid, error_messages)
        """
        errors = []
        resource_type = resource.get('resourceType')
        resource_id = resource.get('id')

        if not resource_type:
            errors.append("Missing resourceType")
            return False, errors

        if not resource_id:
            errors.append("Missing id")
            return False, errors

        if resource_type not in self.SUPPORTED_RESOURCES:
            errors.append(f"Unsupported resourceType: {resource_type}")
            return False, errors

        # Resource-specific validation
        if resource_type == 'Patient':
            if not resource.get('name'):
                errors.append("Patient missing name")
            # birthDate and gender are optional

        elif resource_type == 'Observation':
            if not resource.get('status'):
                errors.append("Observation missing status")
            if not resource.get('code'):
                errors.append("Observation missing code")
            if not (resource.get('effectiveDateTime') or resource.get('effectivePeriod')):
                errors.append("Observation missing effectiveDateTime or effectivePeriod")

        elif resource_type == 'Encounter':
            if not resource.get('status'):
                errors.append("Encounter missing status")

        elif resource_type == 'Condition':
            if not resource.get('code'):
                errors.append("Condition missing code")

        # Check for patient reference
        patient_id = self.derive_patient_id(resource)
        if not patient_id:
            errors.append("Cannot derive patientId from resource")

        return len(errors) == 0, errors

    def process_resource(self, resource: Dict[str, Any]) -> bool:
        """
        Process a single FHIR resource

        Returns:
            True if successfully processed, False otherwise
        """
        self.stats['processed'] += 1

        # Validate resource
        is_valid, errors = self.validate_resource(resource)
        if not is_valid:
            logger.warning(f"Validation failed for {resource.get('resourceType')}/{resource.get('id')}: {errors}")
            self._store_dead_letter(resource, 'validation_error', errors)
            self.stats['errors'] += 1
            return False

        # Derive patient ID
        patient_id = self.derive_patient_id(resource)
        if not patient_id:
            logger.warning(f"Cannot derive patientId for {resource.get('resourceType')}/{resource.get('id')}")
            self._store_dead_letter(resource, 'no_patient_reference')
            self.stats['errors'] += 1
            return False

        # Prepare document for storage
        resource_type = resource['resourceType']
        collection_name = self.SUPPORTED_RESOURCES[resource_type]

        # Create denormalized document
        doc = {
            'resourceType': resource_type,
            'id': resource['id'],
            'patientId': patient_id,
            'resource': resource,  # Store full resource
        }

        # Extract common top-level fields
        meta = resource.get('meta', {})
        doc.update({
            'meta.versionId': meta.get('versionId'),
            'meta.lastUpdated': meta.get('lastUpdated'),
            'status': resource.get('status'),
            'code': resource.get('code'),
            'category': resource.get('category'),
            'effectiveDateTime': resource.get('effectiveDateTime'),
            'issued': resource.get('issued'),
            'occurrenceDateTime': resource.get('occurrenceDateTime'),
            'onsetDateTime': resource.get('onsetDateTime'),
            'authoredOn': resource.get('authoredOn'),
            'recordedDate': resource.get('recordedDate'),
        })

        # Extract encounter reference if present
        encounter_ref = resource.get('encounter', {}).get('reference')
        if encounter_ref and encounter_ref.startswith('Encounter/'):
            doc['encounterId'] = encounter_ref.split('/', 1)[1]

        # Upsert document
        try:
            collection = self.db[collection_name]
            result = collection.update_one(
                {'resourceType': resource_type, 'id': resource['id']},
                {'$set': doc},
                upsert=True
            )

            if result.upserted_id:
                self.stats['inserted'] += 1
            else:
                self.stats['updated'] += 1

            return True

        except Exception as e:
            logger.error(f"Failed to store {resource_type}/{resource['id']}: {e}")
            self._store_dead_letter(resource, 'storage_error', [str(e)])
            self.stats['errors'] += 1
            return False

    def _store_dead_letter(self, resource: Dict[str, Any], reason: str, details: List[str] = None):
        """Store failed resource in dead letter collection"""
        doc = {
            'resourceType': resource.get('resourceType'),
            'id': resource.get('id'),
            'reason': reason,
            'details': details or [],
            'timestamp': datetime.utcnow(),
            'resource': resource
        }

        try:
            self.db['dead_fhir'].insert_one(doc)
            self.stats['dead_letter'] += 1
        except Exception as e:
            logger.error(f"Failed to store dead letter: {e}")

    def process_batch(self, resources: List[Dict[str, Any]], batch_size: int = 500) -> Dict[str, int]:
        """
        Process a batch of resources efficiently

        Args:
            resources: List of FHIR resources
            batch_size: Number of resources per batch

        Returns:
            Processing statistics
        """
        logger.info(f"Processing {len(resources)} resources in batches of {batch_size}")

        for i in range(0, len(resources), batch_size):
            batch = resources[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(resources) + batch_size - 1)//batch_size}")

            for resource in batch:
                self.process_resource(resource)

        return self.stats.copy()

    def process_directory(self, input_dir: str):
        """
        Process all JSON files in a directory

        Args:
            input_dir: Path to directory containing JSON files
        """
        input_path = Path(input_dir)
        if not input_path.exists():
            raise FileNotFoundError(f"Input directory not found: {input_dir}")

        json_files = list(input_path.glob('*.json'))
        logger.info(f"Found {len(json_files)} JSON files in {input_dir}")

        total_resources = 0
        for json_file in json_files:
            logger.info(f"Processing {json_file.name}")
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # Handle both single resource and array of resources
                if isinstance(data, list):
                    resources = data
                else:
                    resources = [data]

                self.process_batch(resources)
                total_resources += len(resources)

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {json_file}: {e}")
            except Exception as e:
                logger.error(f"Error processing {json_file}: {e}")

        logger.info(f"Processed {total_resources} resources from {len(json_files)} files")

    def print_summary(self):
        """Print processing summary and example queries"""
        print("\n" + "="*60)
        print("FHIR INGESTION SUMMARY")
        print("="*60)
        print(f"Total processed: {self.stats['processed']}")
        print(f"Inserted: {self.stats['inserted']}")
        print(f"Updated: {self.stats['updated']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"Dead letter: {self.stats['dead_letter']}")

        # Collection counts
        print("\nCOLLECTION COUNTS:")
        for resource_type, collection_name in self.SUPPORTED_RESOURCES.items():
            count = self.db[collection_name].count_documents({})
            print(f"  {collection_name}: {count}")

        dead_count = self.db['dead_fhir'].count_documents({})
        print(f"  dead_fhir: {dead_count}")

        print("\nEXAMPLE QUERIES:")
        print("# Find all resources for a patient:")
        print('db.patients.findOne({ patientId: "<patientId>" })')
        print("# Or find all observations for a patient:")
        print('db.observations.find({ patientId: "<patientId>" })')
        print("# Get patient with all their observations:")
        print('''
patient = db.patients.findOne({ id: "<patientId>" })
observations = db.observations.find({ patientId: patient.id }).toArray()
        ''')

    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


# Flask app for HTTP endpoint
app = Flask(__name__)
ingestor = None

@app.route('/ingest', methods=['POST'])
def ingest_resources():
    """HTTP endpoint to ingest FHIR resources"""
    try:
        data = request.get_json()
        if not isinstance(data, list):
            return jsonify({'error': 'Expected array of FHIR resources'}), 400

        stats = ingestor.process_batch(data)
        return jsonify({
            'status': 'success',
            'processed': stats['processed'],
            'inserted': stats['inserted'],
            'updated': stats['updated'],
            'errors': stats['errors']
        })

    except Exception as e:
        logger.error(f"HTTP ingestion error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()})


def main():
    """Main CLI entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='FHIR R4 Resource Ingestor')
    parser.add_argument('--input-dir', help='Directory containing JSON files')
    parser.add_argument('--http', action='store_true', help='Run HTTP server mode')
    parser.add_argument('--port', type=int, default=5000, help='HTTP server port')

    args = parser.parse_args()

    # Initialize ingestor
    global ingestor
    ingestor = FHIRIngestor()
    ingestor.connect()
    ingestor.setup_indexes()

    try:
        if args.http:
            # HTTP server mode
            logger.info(f"Starting HTTP server on port {args.port}")
            app.run(host='0.0.0.0', port=args.port, debug=False)

        elif args.input_dir:
            # File processing mode
            ingestor.process_directory(args.input_dir)
            ingestor.print_summary()

        else:
            parser.print_help()

    finally:
        ingestor.close()


if __name__ == '__main__':
    main()