#!/usr/bin/env python3
"""
Simple FHIR Data Inserter
Inserts FHIR R4 resources into MongoDB with patientId linking
"""

import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def insert_fhir_data():
    """Insert sample FHIR data with patientId linking"""

    # MongoDB connection
    mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    client = MongoClient(mongo_uri)
    db = client['fhir_db']  # Use a simple database name

    print(f"Connected to MongoDB: {mongo_uri.replace(mongo_uri.split('@')[0].split('//')[1] if '@' in mongo_uri else '', '***:***@')}")

    # Sample FHIR R4 Patient
    patient = {
        "resourceType": "Patient",
        "id": "patient-001",
        "patientId": "patient-001",  # Self-reference for consistency
        "name": [{
            "family": "Doe",
            "given": ["John"],
            "use": "official"
        }],
        "gender": "male",
        "birthDate": "1980-01-01",
        "address": [{
            "line": ["123 Main St"],
            "city": "Anytown",
            "state": "CA",
            "postalCode": "12345",
            "country": "USA"
        }],
        "telecom": [{
            "system": "phone",
            "value": "555-123-4567",
            "use": "home"
        }]
    }

    # Sample FHIR R4 Observation (linked to patient)
    observation = {
        "resourceType": "Observation",
        "id": "obs-001",
        "patientId": "patient-001",  # Links to patient
        "status": "final",
        "category": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                "code": "vital-signs",
                "display": "Vital Signs"
            }]
        }],
        "code": {
            "coding": [{
                "system": "http://loinc.org",
                "code": "8480-6",
                "display": "Systolic blood pressure"
            }]
        },
        "subject": {
            "reference": "Patient/patient-001"  # FHIR reference
        },
        "effectiveDateTime": "2023-01-01T10:00:00Z",
        "valueQuantity": {
            "value": 120,
            "unit": "mmHg",
            "system": "http://unitsofmeasure.org",
            "code": "mm[Hg]"
        }
    }

    # Sample FHIR R4 Condition (linked to patient)
    condition = {
        "resourceType": "Condition",
        "id": "cond-001",
        "patientId": "patient-001",  # Links to patient
        "clinicalStatus": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                "code": "active",
                "display": "Active"
            }]
        },
        "verificationStatus": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                "code": "confirmed",
                "display": "Confirmed"
            }]
        },
        "category": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                "code": "problem-list-item",
                "display": "Problem List Item"
            }]
        }],
        "code": {
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": "38341003",
                "display": "Hypertension"
            }]
        },
        "subject": {
            "reference": "Patient/patient-001"  # FHIR reference
        },
        "onsetDateTime": "2022-01-01T00:00:00Z",
        "recordedDate": "2022-01-01T00:00:00Z"
    }

    # Sample FHIR R4 Encounter (linked to patient)
    encounter = {
        "resourceType": "Encounter",
        "id": "enc-001",
        "patientId": "patient-001",  # Links to patient
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "AMB",
            "display": "ambulatory"
        },
        "type": [{
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": "185345009",
                "display": "Encounter for symptom"
            }]
        }],
        "subject": {
            "reference": "Patient/patient-001"  # FHIR reference
        },
        "period": {
            "start": "2023-01-01T09:00:00Z",
            "end": "2023-01-01T10:30:00Z"
        },
        "reasonCode": [{
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": "38341003",
                "display": "Hypertension"
            }]
        }]
    }

    # Insert data
    try:
        # Insert patient
        db.patients.insert_one(patient)
        print("✅ Inserted patient")

        # Insert observation
        db.observations.insert_one(observation)
        print("✅ Inserted observation")

        # Insert condition
        db.conditions.insert_one(condition)
        print("✅ Inserted condition")

        # Insert encounter
        db.encounters.insert_one(encounter)
        print("✅ Inserted encounter")

        print("\n🎉 All FHIR data inserted successfully!")
        print("\n📊 Data Summary:")
        print(f"   • Patients: {db.patients.count_documents({})}")
        print(f"   • Observations: {db.observations.count_documents({})}")
        print(f"   • Conditions: {db.conditions.count_documents({})}")
        print(f"   • Encounters: {db.encounters.count_documents({})}")

        print("\n🔗 Query Examples:")

        # Query all resources for a patient
        print("\n# Find all resources for patient-001:")
        patient_resources = {
            "patients": list(db.patients.find({"patientId": "patient-001"})),
            "observations": list(db.observations.find({"patientId": "patient-001"})),
            "conditions": list(db.conditions.find({"patientId": "patient-001"})),
            "encounters": list(db.encounters.find({"patientId": "patient-001"}))
        }

        for collection, docs in patient_resources.items():
            print(f"   {collection}: {len(docs)} documents")
            if docs:
                print(f"   Sample {collection[:-1]} ID: {docs[0]['id']}")

        print("\n# Direct MongoDB queries:")
        print('db.patients.findOne({ "patientId": "patient-001" })')
        print('db.observations.find({ "patientId": "patient-001" })')
        print('db.conditions.find({ "patientId": "patient-001" })')
        print('db.encounters.find({ "patientId": "patient-001" })')

    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        raise
    finally:
        client.close()
        print("\n🔌 MongoDB connection closed")

if __name__ == "__main__":
    insert_fhir_data()