#!/usr/bin/env python3
"""
Synthetic FHIR Medical Data Generator
Generates comprehensive synthetic medical data using FHIR standards
"""

import random
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
import uuid
from faker import Faker
import pymongo
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class FHIRDataGenerator:
    def __init__(self, mongo_uri: str = None, database_name: str = "medical_db"):
        """
        Initialize the FHIR data generator

        Args:
            mongo_uri: MongoDB connection string
            database_name: Name of the database to use
        """
        self.fake = Faker(['en_US'])
        self.mongo_uri = mongo_uri or os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.database_name = database_name
        self.client = None
        self.db = None

        # FHIR coding systems
        self.snomed_codes = {
            'conditions': {
                '38341003': 'Hypertension',
                '44054006': 'Diabetes mellitus type 2',
                '195967001': 'Asthma',
                '414545008': 'Ischemic heart disease',
                '35489007': 'Depressive disorder',
                '49601007': 'Disorder of cardiovascular system',
                '239872002': 'Osteoarthritis',
                '198992004': 'Anxiety disorder',
                '431855005': 'Chronic kidney disease',
                '40055000': 'Chronic obstructive pulmonary disease',
                '13645005': 'COPD',
                '90708001': 'Kidney stones',
                '128613002': 'Seizure disorder',
                '363346000': 'Cancer',
                '73211009': 'Diabetes mellitus',
                '22298006': 'Myocardial infarction',
                '230690007': 'Stroke'
            },
            'encounters': {
                '185345009': 'Encounter for symptom',
                '185349003': 'Encounter for check up',
                '308335008': 'Patient encounter procedure',
                '270427003': 'Patient-initiated encounter',
                '270430005': 'Provider-initiated encounter'
            }
        }

        self.loinc_codes = {
            'vitals': {
                '8480-6': 'Systolic blood pressure',
                '8462-4': 'Diastolic blood pressure',
                '8867-4': 'Heart rate',
                '39156-5': 'Body mass index (BMI)',
                '29463-7': 'Body weight',
                '8302-2': 'Body height',
                '8310-5': 'Body temperature',
                '9279-1': 'Respiratory rate'
            },
            'labs': {
                '2093-3': 'Total cholesterol',
                '2085-9': 'HDL cholesterol',
                '2571-8': 'Triglycerides',
                '2160-0': 'Creatinine',
                '1920-8': 'Aspartate aminotransferase',
                '1742-6': 'Alanine aminotransferase',
                '2951-2': 'Sodium',
                '2823-3': 'Potassium',
                '2075-0': 'Chloride',
                '1963-8': 'Bicarbonate',
                '17861-6': 'Hemoglobin',
                '789-8': 'Erythrocytes',
                '6690-2': 'Leukocytes',
                '5905-5': 'Platelets'
            }
        }

        self.rxnorm_codes = {
            '866436': 'Lisinopril 10 MG Oral Tablet',
            '860975': 'Metformin 500 MG Oral Tablet',
            '745679': 'Albuterol 90 MCG/ACTUAT Inhaler',
            '197361': 'Aspirin 81 MG Oral Tablet',
            '198440': 'Ibuprofen 600 MG Oral Tablet',
            '1049630': 'Sertraline 50 MG Oral Tablet',
            '197320': 'Atorvastatin 20 MG Oral Tablet',
            '866924': 'Simvastatin 20 MG Oral Tablet',
            '308135': 'Warfarin 5 MG Oral Tablet',
            '197517': 'Furosemide 40 MG Oral Tablet',
            '849574': 'Omeprazole 20 MG Oral Capsule',
            '849727': 'Prednisone 10 MG Oral Tablet',
            '849296': 'Levothyroxine 75 MCG Oral Tablet',
            '849298': 'Levothyroxine 100 MCG Oral Tablet',
            '849300': 'Levothyroxine 125 MCG Oral Tablet'
        }

    def connect_to_mongodb(self):
        """Connect to MongoDB with fallback options"""
        try:
            print(f"🔌 Attempting to connect to MongoDB...")
            print(f"   URI: {self.mongo_uri.replace(self.mongo_uri.split('@')[0].split('//')[1], '***:***@') if '@' in self.mongo_uri else self.mongo_uri}")

            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            # Test the connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            print(f"✅ Connected to MongoDB database: {self.database_name}")
        except Exception as e:
            print(f"❌ Failed to connect to MongoDB: {e}")

            # Try local MongoDB as fallback
            print("🔄 Attempting fallback to local MongoDB...")
            try:
                local_uri = "mongodb://localhost:27017/"
                self.client = MongoClient(local_uri, serverSelectionTimeoutMS=2000)
                self.client.admin.command('ping')
                self.db = self.client[self.database_name]
                print(f"✅ Connected to local MongoDB database: {self.database_name}")
                print("⚠️  Using local MongoDB - make sure MongoDB is running locally")
            except Exception as local_e:
                print(f"❌ Local MongoDB connection also failed: {local_e}")
                print("\n🔧 Troubleshooting options:")
                print("   1. Install MongoDB locally: https://docs.mongodb.com/manual/installation/")
                print("   2. Start MongoDB service: mongod")
                print("   3. Or update the MONGO_URI in the script with correct Atlas connection string")
                print("   4. Check your MongoDB Atlas cluster is active and accessible")
                raise Exception("Unable to connect to MongoDB. Please check your connection string or install local MongoDB.")

    def clear_all_data(self):
        """Clear all existing data from the database"""
        try:
            collections = self.db.list_collection_names()
            for collection_name in collections:
                self.db[collection_name].drop()
                print(f"🗑️ Dropped collection: {collection_name}")

            print("✅ All existing data cleared from database")
        except Exception as e:
            print(f"❌ Failed to clear data: {e}")
            raise

    def generate_patient_id(self, index: int) -> str:
        """Generate a unique patient ID"""
        return f"{index:03d}"

    def generate_patient(self, patient_id: str) -> Dict[str, Any]:
        """Generate a synthetic FHIR Patient resource"""
        gender = random.choice(['male', 'female'])
        birth_date = self.fake.date_of_birth(minimum_age=18, maximum_age=90)

        # Generate realistic names based on gender
        if gender == 'male':
            first_name = self.fake.first_name_male()
        else:
            first_name = self.fake.first_name_female()

        last_name = self.fake.last_name()

        # Generate address
        address = {
            "line": [self.fake.street_address()],
            "city": self.fake.city(),
            "state": self.fake.state_abbr(),
            "postalCode": self.fake.zipcode(),
            "country": "USA"
        }

        # Generate telecom
        telecom = [{
            "system": "phone",
            "value": self.fake.phone_number(),
            "use": "home"
        }]

        # Add mobile phone sometimes
        if random.random() < 0.7:
            telecom.append({
                "system": "phone",
                "value": self.fake.phone_number(),
                "use": "mobile"
            })

        # Generate marital status
        marital_status_options = [
            {"coding": [{"system": "http://hl7.org/fhir/v3/MaritalStatus", "code": "S", "display": "Single"}]},
            {"coding": [{"system": "http://hl7.org/fhir/v3/MaritalStatus", "code": "M", "display": "Married"}]},
            {"coding": [{"system": "http://hl7.org/fhir/v3/MaritalStatus", "code": "D", "display": "Divorced"}]},
            {"coding": [{"system": "http://hl7.org/fhir/v3/MaritalStatus", "code": "W", "display": "Widowed"}]}
        ]

        patient = {
            "resourceType": "Patient",
            "id": patient_id,
            "patientId": patient_id,  # Add patientId for cross-collection querying consistency
            "identifier": [{
                "system": "http://hospital.org/patient-id",
                "value": patient_id
            }],
            "name": [{
                "family": last_name,
                "given": [first_name],
                "use": "official"
            }],
            "gender": gender,
            "birthDate": birth_date.strftime("%Y-%m-%d"),
            "address": [address],
            "telecom": telecom,
            "maritalStatus": random.choice(marital_status_options),
            "communication": [{
                "language": {
                    "coding": [{
                        "system": "urn:ietf:bcp:47",
                        "code": "en-US",
                        "display": "English (United States)"
                    }]
                },
                "preferred": True
            }]
        }

        return patient

    def generate_observation(self, patient_id: str, obs_id: str) -> Dict[str, Any]:
        """Generate a synthetic FHIR Observation resource"""
        # Choose observation type
        obs_type = random.choice(['vital', 'lab'])
        if obs_type == 'vital':
            code_options = list(self.loinc_codes['vitals'].items())
        else:
            code_options = list(self.loinc_codes['labs'].items())

        code, display = random.choice(code_options)

        # Generate realistic values based on the observation type
        if code == '8480-6':  # Systolic BP
            value = random.randint(90, 180)
            unit = "mmHg"
        elif code == '8462-4':  # Diastolic BP
            value = random.randint(60, 110)
            unit = "mmHg"
        elif code == '8867-4':  # Heart rate
            value = random.randint(60, 100)
            unit = "beats/min"
        elif code == '39156-5':  # BMI
            value = round(random.uniform(18.5, 40.0), 1)
            unit = "kg/m2"
        elif code == '29463-7':  # Weight
            value = round(random.uniform(45, 150), 1)
            unit = "kg"
        elif code == '8302-2':  # Height
            value = random.randint(150, 200)
            unit = "cm"
        elif code == '2093-3':  # Total cholesterol
            value = random.randint(120, 300)
            unit = "mg/dL"
        elif code == '2085-9':  # HDL
            value = random.randint(30, 80)
            unit = "mg/dL"
        elif code == '2160-0':  # Creatinine
            value = round(random.uniform(0.5, 2.0), 1)
            unit = "mg/dL"
        else:
            value = round(random.uniform(10, 200), 1)
            unit = "mg/dL"

        # Generate effective date (within last year)
        effective_date = self.fake.date_time_between(start_date='-1y', end_date='now')

        observation = {
            "resourceType": "Observation",
            "id": obs_id,
            "patientId": patient_id,  # Add patientId for cross-collection querying
            "status": "final",
            "category": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": obs_type,
                    "display": obs_type.title()
                }]
            }],
            "code": {
                "coding": [{
                    "system": "http://loinc.org",
                    "code": code,
                    "display": display
                }]
            },
            "subject": {
                "reference": f"Patient/{patient_id}"
            },
            "effectiveDateTime": effective_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
            "valueQuantity": {
                "value": value,
                "unit": unit,
                "system": "http://unitsofmeasure.org",
                "code": unit.replace("/", "").replace(" ", "")
            }
        }

        return observation

    def generate_condition(self, patient_id: str, condition_id: str) -> Dict[str, Any]:
        """Generate a synthetic FHIR Condition resource"""
        condition_codes = list(self.snomed_codes['conditions'].items())
        code, display = random.choice(condition_codes)

        # Generate onset date (within last 5 years)
        onset_date = self.fake.date_time_between(start_date='-5y', end_date='now')

        # Determine clinical status
        clinical_status = random.choice(['active', 'resolved', 'inactive'])

        # Determine verification status
        verification_status = random.choice(['confirmed', 'provisional'])

        condition = {
            "resourceType": "Condition",
            "id": condition_id,
            "patientId": patient_id,  # Add patientId for cross-collection querying
            "clinicalStatus": {
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                    "code": clinical_status,
                    "display": clinical_status.title()
                }]
            },
            "verificationStatus": {
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                    "code": verification_status,
                    "display": verification_status.title()
                }]
            },
            "category": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                    "code": "problem-list-item",
                    "display": "Problem List Item"
                }]
            }],
            "severity": {
                "coding": [{
                    "system": "http://snomed.info/sct",
                    "code": random.choice(["24484000", "6736007", "255604002"]),
                    "display": random.choice(["Severe", "Moderate", "Mild"])
                }]
            },
            "code": {
                "coding": [{
                    "system": "http://snomed.info/sct",
                    "code": code,
                    "display": display
                }]
            },
            "subject": {
                "reference": f"Patient/{patient_id}"
            },
            "onsetDateTime": onset_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
            "recordedDate": onset_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        }

        return condition

    def generate_medication_request(self, patient_id: str, med_id: str) -> Dict[str, Any]:
        """Generate a synthetic FHIR MedicationRequest resource"""
        medication_codes = list(self.rxnorm_codes.items())
        code, display = random.choice(medication_codes)

        # Generate authored date (within last 6 months)
        authored_date = self.fake.date_time_between(start_date='-6M', end_date='now')

        # Generate dosage based on medication type
        if 'Tablet' in display:
            dose_value = random.choice([10, 25, 50, 100, 200])
            dose_unit = "mg"
        elif 'Inhaler' in display:
            dose_value = 2
            dose_unit = "puffs"
        elif 'Capsule' in display:
            dose_value = 20
            dose_unit = "mg"
        else:
            dose_value = 1
            dose_unit = "tablet"

        medication_request = {
            "resourceType": "MedicationRequest",
            "id": med_id,
            "patientId": patient_id,  # Add patientId for cross-collection querying
            "status": random.choice(["active", "completed", "cancelled"]),
            "intent": "order",
            "medicationCodeableConcept": {
                "coding": [{
                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                    "code": code,
                    "display": display
                }]
            },
            "subject": {
                "reference": f"Patient/{patient_id}"
            },
            "encounter": {
                "reference": f"Encounter/enc-{patient_id.replace('pat-', '')}"
            },
            "authoredOn": authored_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
            "requester": {
                "reference": f"Practitioner/pract-{patient_id.replace('pat-', '')}"
            },
            "dosageInstruction": [{
                "text": f"Take {dose_value} {dose_unit} by mouth once daily",
                "timing": {
                    "repeat": {
                        "frequency": 1,
                        "period": 1,
                        "periodUnit": "d"
                    }
                },
                "doseAndRate": [{
                    "doseQuantity": {
                        "value": dose_value,
                        "unit": dose_unit,
                        "system": "http://unitsofmeasure.org",
                        "code": dose_unit
                    }
                }]
            }],
            "dispenseRequest": {
                "numberOfRepeatsAllowed": random.randint(1, 5),
                "quantity": {
                    "value": random.randint(10, 90),
                    "unit": "tablets",
                    "system": "http://unitsofmeasure.org",
                    "code": "{tbl}"
                }
            }
        }

        return medication_request

    def generate_encounter(self, patient_id: str, encounter_id: str) -> Dict[str, Any]:
        """Generate a synthetic FHIR Encounter resource"""
        encounter_codes = list(self.snomed_codes['encounters'].items())
        code, display = random.choice(encounter_codes)

        # Generate encounter period
        start_date = self.fake.date_time_between(start_date='-1y', end_date='now')
        duration_hours = random.randint(15, 120)  # 15 minutes to 2 hours
        end_date = start_date + timedelta(minutes=duration_hours)

        encounter = {
            "resourceType": "Encounter",
            "id": encounter_id,
            "patientId": patient_id,  # Add patientId for cross-collection querying
            "status": random.choice(["finished", "in-progress", "cancelled"]),
            "class": {
                "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                "code": "AMB",
                "display": "ambulatory"
            },
            "type": [{
                "coding": [{
                    "system": "http://snomed.info/sct",
                    "code": code,
                    "display": display
                }]
            }],
            "subject": {
                "reference": f"Patient/{patient_id}"
            },
            "participant": [{
                "individual": {
                    "reference": f"Practitioner/pract-{patient_id.replace('pat-', '')}"
                }
            }],
            "period": {
                "start": start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
                "end": end_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
            },
            "reasonCode": [{
                "coding": [{
                    "system": "http://snomed.info/sct",
                    "code": random.choice(list(self.snomed_codes['conditions'].keys())),
                    "display": random.choice(list(self.snomed_codes['conditions'].values()))
                }]
            }],
            "serviceProvider": {
                "reference": "Organization/org-001"
            }
        }

        return encounter

    def generate_all_data(self, num_patients: int = 300):
        """
        Generate comprehensive synthetic FHIR medical data

        Args:
            num_patients: Number of patients to generate
        """
        print(f"🚀 Starting generation of {num_patients} patients with associated medical data...")

        patients = []
        observations = []
        conditions = []
        medication_requests = []
        encounters = []

        # Generate patients
        print("📝 Generating patients...")
        for i in range(1, num_patients + 1):
            patient_id = self.generate_patient_id(i)
            patient = self.generate_patient(patient_id)
            patients.append(patient)

            # Generate associated data for each patient
            # Observations (2-5 per patient)
            num_obs = random.randint(2, 5)
            for j in range(num_obs):
                obs_id = f"obs-{i:03d}-{j+1}"
                observation = self.generate_observation(patient_id, obs_id)
                observations.append(observation)

            # Conditions (0-3 per patient)
            num_conditions = random.randint(0, 3)
            for j in range(num_conditions):
                condition_id = f"cond-{i:03d}-{j+1}"
                condition = self.generate_condition(patient_id, condition_id)
                conditions.append(condition)

            # Medication requests (0-4 per patient)
            num_meds = random.randint(0, 4)
            for j in range(num_meds):
                med_id = f"medreq-{i:03d}-{j+1}"
                med_request = self.generate_medication_request(patient_id, med_id)
                medication_requests.append(med_request)

            # Encounters (1-3 per patient)
            num_encounters = random.randint(1, 3)
            for j in range(num_encounters):
                encounter_id = f"enc-{i:03d}-{j+1}"
                encounter = self.generate_encounter(patient_id, encounter_id)
                encounters.append(encounter)

        # Insert data into MongoDB
        print("💾 Inserting data into MongoDB...")

        if patients:
            self.db.patients.insert_many(patients)
            print(f"✅ Inserted {len(patients)} patients")

        if observations:
            self.db.observations.insert_many(observations)
            print(f"✅ Inserted {len(observations)} observations")

        if conditions:
            self.db.conditions.insert_many(conditions)
            print(f"✅ Inserted {len(conditions)} conditions")

        if medication_requests:
            self.db.medication_requests.insert_many(medication_requests)
            print(f"✅ Inserted {len(medication_requests)} medication requests")

        if encounters:
            self.db.encounters.insert_many(encounters)
            print(f"✅ Inserted {len(encounters)} encounters")

        print("🎉 Synthetic FHIR medical data generation completed!")
        print(f"📊 Summary:")
        print(f"   • Patients: {len(patients)}")
        print(f"   • Observations: {len(observations)}")
        print(f"   • Conditions: {len(conditions)}")
        print(f"   • Medication Requests: {len(medication_requests)}")
        print(f"   • Encounters: {len(encounters)}")

    def run(self, num_patients: int = 300):
        """Main execution method"""
        try:
            print("🔌 Connecting to MongoDB...")
            self.connect_to_mongodb()

            print("🗑️ Clearing existing data...")
            self.clear_all_data()

            print("🏥 Generating synthetic FHIR medical data...")
            self.generate_all_data(num_patients)

            print("✅ All operations completed successfully!")

        except Exception as e:
            print(f"❌ Error during execution: {e}")
            raise
        finally:
            if self.client:
                self.client.close()
                print("🔌 MongoDB connection closed")


def main():
    """Main function with improved configuration"""
    import argparse

    parser = argparse.ArgumentParser(description='Generate synthetic FHIR medical data')
    parser.add_argument('--patients', type=int, help='Number of patients to generate')
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')

    args = parser.parse_args()

    print("🏥 Synthetic FHIR Medical Data Generator")
    print("=" * 50)

    # Configuration - Try environment variables first, then defaults
    MONGO_URI = os.getenv('MONGODB_URI')
    if not MONGO_URI:
        # Default options - user can choose
        print("\n📋 MongoDB Connection Options:")
        print("   1. MongoDB Atlas (Cloud)")
        print("   2. Local MongoDB")
        print("   3. Custom connection string")

        choice = input("\nChoose connection option (1-3) [2]: ").strip() or "2"

        if choice == "1":
            atlas_uri = input("Enter your MongoDB Atlas connection string: ").strip()
            if atlas_uri:
                MONGO_URI = atlas_uri
            else:
                print("❌ No Atlas URI provided, falling back to local")
                MONGO_URI = "mongodb://localhost:27017/"
        elif choice == "3":
            custom_uri = input("Enter custom MongoDB connection string: ").strip()
            if custom_uri:
                MONGO_URI = custom_uri
            else:
                print("❌ No custom URI provided, falling back to local")
                MONGO_URI = "mongodb://localhost:27017/"
        else:
            MONGO_URI = "mongodb://localhost:27017/"
            print("ℹ️  Using local MongoDB connection")

    DATABASE_NAME = os.getenv('DATABASE_NAME', 'medical_db')

    # Get number of patients - prioritize: args > env > default
    if args.patients:
        NUM_PATIENTS = args.patients
    else:
        num_patients_env = os.getenv('NUM_PATIENTS')
        if num_patients_env:
            try:
                NUM_PATIENTS = int(num_patients_env)
            except ValueError:
                print(f"❌ Invalid NUM_PATIENTS in environment: {num_patients_env}, using default: 100")
                NUM_PATIENTS = 100
        else:
            NUM_PATIENTS = 100  # Changed default to 100

    print(f"\n⚙️  Configuration:")
    print(f"   • MongoDB URI: {MONGO_URI.replace(MONGO_URI.split('@')[0].split('//')[1] if '@' in MONGO_URI else '', '***:***@') if '@' in MONGO_URI else MONGO_URI}")
    print(f"   • Database: {DATABASE_NAME}")
    print(f"   • Patients to generate: {NUM_PATIENTS}")

    if not args.force:
        confirm = input("\n🚨 This will DELETE ALL existing data in the database. Continue? (y/N): ").strip().lower()
        if confirm not in ['y', 'yes']:
            print("❌ Operation cancelled by user")
            return

    # Create and run the generator
    try:
        generator = FHIRDataGenerator(
            mongo_uri=MONGO_URI,
            database_name=DATABASE_NAME
        )

        generator.run(num_patients=NUM_PATIENTS)
        print("\n🎉 Synthetic FHIR medical data generation completed successfully!")

        # Print query examples
        print("\n🔍 Query Examples:")
        print("# Find all resources for a patient:")
        print('db.patients.findOne({ "patientId": "001" })')
        print('db.observations.find({ "patientId": "001" })')
        print('db.conditions.find({ "patientId": "001" })')
        print('db.encounters.find({ "patientId": "001" })')
        print('db.medication_requests.find({ "patientId": "001" })')

    except Exception as e:
        print(f"\n❌ Error during execution: {e}")
        print("\n🔧 Troubleshooting:")
        print("   • For MongoDB Atlas: Check your cluster is active and connection string is correct")
        print("   • For local MongoDB: Make sure MongoDB is installed and running")
        print("   • Check network connectivity and firewall settings")


if __name__ == "__main__":
    main()
