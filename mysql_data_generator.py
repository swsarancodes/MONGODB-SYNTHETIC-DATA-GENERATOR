#!/usr/bin/env python3
"""
MySQL Synthetic Medical Data Generator
Generates comprehensive synthetic medical data using the provided schema
"""

import random
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
import uuid
from faker import Faker
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class MySQLDataGenerator:
    def __init__(self, mysql_config: dict = None):
        """
        Initialize the MySQL data generator

        Args:
            mysql_config: MySQL connection configuration dictionary
        """
        self.fake = Faker(['en_US'])
        self.connection = None
        self.mysql_config = mysql_config or {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_PASSWORD', ''),
            'database': os.getenv('MYSQL_DATABASE', 'medical_db')
        }

        # FHIR coding systems (keeping for reference)
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

    def connect_to_mysql(self):
        """Connect to MySQL database"""
        try:
            print("🔌 Attempting to connect to MySQL...")
            print(f"   Host: {self.mysql_config['host']}:{self.mysql_config['port']}")
            print(f"   Database: {self.mysql_config['database']}")
            print(f"   User: {self.mysql_config['user']}")

            self.connection = mysql.connector.connect(**self.mysql_config)
            if self.connection.is_connected():
                print("✅ Connected to MySQL database")
        except Error as e:
            print(f"❌ Failed to connect to MySQL: {e}")
            raise

    def create_tables(self):
        """Create all tables according to the provided schema"""
        tables_sql = {
            'allergies': """
                CREATE TABLE IF NOT EXISTS allergies (
                    REGISTERED_AT DATE,
                    STOP TEXT,
                    PATIENT TEXT,
                    ENCOUNTER TEXT,
                    CODE BIGINT,
                    SYSTEM TEXT,
                    DESCRIPTION TEXT,
                    TYPE TEXT,
                    CATEGORY TEXT,
                    REACTION1 BIGINT,
                    DESCRIPTION1 TEXT,
                    SEVERITY1 TEXT,
                    REACTION2 BIGINT,
                    DESCRIPTION2 TEXT,
                    SEVERITY2 TEXT
                )
            """,
            'careplans': """
                CREATE TABLE IF NOT EXISTS careplans (
                    ID TEXT,
                    REGISTERED_AT DATE,
                    STOP DATE,
                    PATIENT TEXT,
                    ENCOUNTER TEXT,
                    CODE BIGINT,
                    DESCRIPTION TEXT,
                    REASONCODE BIGINT,
                    REASONDESCRIPTION TEXT
                )
            """,
            'claims': """
                CREATE TABLE IF NOT EXISTS claims (
                    ID TEXT,
                    PATIENTID TEXT,
                    PROVIDERID TEXT,
                    PRIMARYPATIENTINSURANCEID TEXT,
                    SECONDARYPATIENTINSURANCEID TEXT,
                    DEPARTMENTID BIGINT,
                    PATIENTDEPARTMENTID BIGINT,
                    DIAGNOSIS1 BIGINT,
                    DIAGNOSIS2 BIGINT,
                    DIAGNOSIS3 BIGINT,
                    DIAGNOSIS4 BIGINT,
                    DIAGNOSIS5 BIGINT,
                    DIAGNOSIS6 BIGINT,
                    DIAGNOSIS7 BIGINT,
                    DIAGNOSIS8 BIGINT,
                    REFERRINGPROVIDERID TEXT,
                    APPOINTMENTID TEXT,
                    CURRENTILLNESSDATE TIMESTAMP,
                    SERVICEDATE TIMESTAMP,
                    SUPERVISINGPROVIDERID TEXT,
                    STATUS1 TEXT,
                    STATUS2 TEXT,
                    STATUSP TEXT,
                    OUTSTANDING1 DECIMAL(38,2),
                    OUTSTANDING2 DECIMAL(38,2),
                    OUTSTANDINGP DECIMAL(38,2),
                    LASTBILLEDDATE1 TIMESTAMP,
                    LASTBILLEDDATE2 TIMESTAMP,
                    LASTBILLEDDATEP TIMESTAMP,
                    HEALTHCARECLAIMTYPEID1 BIGINT,
                    HEALTHCARECLAIMTYPEID2 BIGINT
                )
            """,
            'claims_transactions': """
                CREATE TABLE IF NOT EXISTS claims_transactions (
                    ID TEXT,
                    CLAIMID TEXT,
                    CHARGEID BIGINT,
                    PATIENTID TEXT,
                    TYPE TEXT,
                    AMOUNT DECIMAL(38,2),
                    METHOD TEXT,
                    FROMDATE TIMESTAMP,
                    TODATE TIMESTAMP,
                    PLACEOFSERVICE TEXT,
                    PROCEDURECODE BIGINT,
                    MODIFIER1 TEXT,
                    MODIFIER2 TEXT,
                    DIAGNOSISREF1 BIGINT,
                    DIAGNOSISREF2 BIGINT,
                    DIAGNOSISREF3 BIGINT,
                    DIAGNOSISREF4 BIGINT,
                    UNITS BIGINT,
                    DEPARTMENTID BIGINT,
                    NOTES TEXT,
                    UNITAMOUNT DECIMAL(38,2),
                    TRANSFEROUTID BIGINT,
                    TRANSFERTYPE TEXT,
                    PAYMENTS DECIMAL(38,2),
                    ADJUSTMENTS BIGINT,
                    TRANSFERS DECIMAL(38,2),
                    OUTSTANDING DECIMAL(38,2),
                    APPOINTMENTID TEXT,
                    LINENOTE TEXT,
                    PATIENTINSURANCEID TEXT,
                    FEESCHEDULEID BIGINT,
                    PROVIDERID TEXT,
                    SUPERVISINGPROVIDERID TEXT
                )
            """,
            'conditions': """
                CREATE TABLE IF NOT EXISTS conditions (
                    REGISTERED_AT DATE,
                    STOP DATE,
                    PATIENT TEXT,
                    ENCOUNTER TEXT,
                    SYSTEM TEXT,
                    CODE BIGINT,
                    DESCRIPTION TEXT
                )
            """,
            'devices': """
                CREATE TABLE IF NOT EXISTS devices (
                    REGISTERED_AT TIMESTAMP,
                    STOP TIMESTAMP,
                    PATIENT TEXT,
                    ENCOUNTER TEXT,
                    CODE BIGINT,
                    DESCRIPTION TEXT,
                    UDI TEXT
                )
            """,
            'encounters': """
                CREATE TABLE IF NOT EXISTS encounters (
                    ID TEXT,
                    REGISTERED_AT TIMESTAMP,
                    STOP TIMESTAMP,
                    PATIENT TEXT,
                    ORGANIZATION TEXT,
                    PROVIDER TEXT,
                    PAYER TEXT,
                    ENCOUNTERCLASS TEXT,
                    CODE BIGINT,
                    DESCRIPTION TEXT,
                    BASE_ENCOUNTER_COST DECIMAL(38,2),
                    TOTAL_CLAIM_COST DECIMAL(38,2),
                    PAYER_COVERAGE DECIMAL(38,2),
                    REASONCODE BIGINT,
                    REASONDESCRIPTION TEXT
                )
            """,
            'imaging_studies': """
                CREATE TABLE IF NOT EXISTS imaging_studies (
                    ID TEXT,
                    DATE TIMESTAMP,
                    PATIENT TEXT,
                    ENCOUNTER TEXT,
                    SERIES_UID TEXT,
                    BODYSITE_CODE BIGINT,
                    BODYSITE_DESCRIPTION TEXT,
                    MODALITY_CODE TEXT,
                    MODALITY_DESCRIPTION TEXT,
                    INSTANCE_UID TEXT,
                    SOP_CODE TEXT,
                    SOP_DESCRIPTION TEXT,
                    PROCEDURE_CODE BIGINT
                )
            """,
            'immunizations': """
                CREATE TABLE IF NOT EXISTS immunizations (
                    DATE TIMESTAMP,
                    PATIENT TEXT,
                    ENCOUNTER TEXT,
                    CODE BIGINT,
                    DESCRIPTION TEXT,
                    BASE_COST DECIMAL(38,2)
                )
            """,
            'medications': """
                CREATE TABLE IF NOT EXISTS medications (
                    REGISTERED_AT TIMESTAMP,
                    STOP TIMESTAMP,
                    PATIENT TEXT,
                    PAYER TEXT,
                    ENCOUNTER TEXT,
                    CODE BIGINT,
                    DESCRIPTION TEXT,
                    BASE_COST DECIMAL(38,2),
                    PAYER_COVERAGE DECIMAL(38,2),
                    DISPENSES BIGINT,
                    TOTALCOST DECIMAL(38,2),
                    REASONCODE BIGINT,
                    REASONDESCRIPTION TEXT
                )
            """,
            'observations': """
                CREATE TABLE IF NOT EXISTS observations (
                    C1 TEXT,
                    C2 TEXT,
                    C3 TEXT,
                    C4 TEXT,
                    C5 TEXT,
                    C6 TEXT,
                    C7 TEXT,
                    C8 TEXT,
                    C9 TEXT
                )
            """,
            'organizations': """
                CREATE TABLE IF NOT EXISTS organizations (
                    ID TEXT,
                    NAME TEXT,
                    ADDRESS TEXT,
                    CITY TEXT,
                    STATE TEXT,
                    ZIP BIGINT,
                    LAT DECIMAL(38,15),
                    LON DECIMAL(38,14),
                    PHONE TEXT,
                    REVENUE BIGINT,
                    UTILIZATION BIGINT
                )
            """,
            'patients': """
                CREATE TABLE IF NOT EXISTS patients (
                    ID TEXT,
                    BIRTHDATE DATE,
                    DEATHDATE DATE,
                    SSN TEXT,
                    DRIVERS TEXT,
                    PASSPORT TEXT,
                    PREFIX TEXT,
                    FIRST TEXT,
                    MIDDLE TEXT,
                    LAST TEXT,
                    SUFFIX TEXT,
                    MAIDEN TEXT,
                    MARITAL TEXT,
                    RACE TEXT,
                    ETHNICITY TEXT,
                    GENDER TEXT,
                    BIRTHPLACE TEXT,
                    ADDRESS TEXT,
                    CITY TEXT,
                    STATE TEXT,
                    COUNTY TEXT,
                    FIPS BIGINT,
                    ZIP BIGINT,
                    LAT DECIMAL(38,15),
                    LON DECIMAL(38,14),
                    HEALTHCARE_EXPENSES DECIMAL(38,2),
                    HEALTHCARE_COVERAGE DECIMAL(38,2),
                    INCOME BIGINT
                )
            """,
            'payers': """
                CREATE TABLE IF NOT EXISTS payers (
                    ID TEXT,
                    NAME TEXT,
                    OWNERSHIP TEXT,
                    ADDRESS TEXT,
                    CITY TEXT,
                    STATE_HEADQUARTERED TEXT,
                    ZIP TEXT,
                    PHONE TEXT,
                    AMOUNT_COVERED DECIMAL(38,2),
                    AMOUNT_UNCOVERED DECIMAL(38,2),
                    REVENUE DECIMAL(38,2),
                    COVERED_ENCOUNTERS BIGINT,
                    UNCOVERED_ENCOUNTERS BIGINT,
                    COVERED_MEDICATIONS BIGINT,
                    UNCOVERED_MEDICATIONS BIGINT,
                    COVERED_PROCEDURES BIGINT,
                    UNCOVERED_PROCEDURES BIGINT,
                    COVERED_IMMUNIZATIONS BIGINT,
                    UNCOVERED_IMMUNIZATIONS BIGINT,
                    UNIQUE_CUSTOMERS BIGINT,
                    QOLS_AVG DECIMAL(38,16),
                    MEMBER_MONTHS BIGINT
                )
            """,
            'payer_transitions': """
                CREATE TABLE IF NOT EXISTS payer_transitions (
                    PATIENT TEXT,
                    MEMBERID TEXT,
                    START_DATE TIMESTAMP,
                    END_DATE TIMESTAMP,
                    PAYER TEXT,
                    SECONDARY_PAYER TEXT,
                    PLAN_OWNERSHIP TEXT,
                    OWNER_NAME TEXT
                )
            """,
            'procedures': """
                CREATE TABLE IF NOT EXISTS procedures (
                    REGISTERED_AT TIMESTAMP,
                    STOP TIMESTAMP,
                    PATIENT TEXT,
                    ENCOUNTER TEXT,
                    SYSTEM TEXT,
                    CODE BIGINT,
                    DESCRIPTION TEXT,
                    BASE_COST DECIMAL(38,2),
                    REASONCODE BIGINT,
                    REASONDESCRIPTION TEXT
                )
            """,
            'providers': """
                CREATE TABLE IF NOT EXISTS providers (
                    ID TEXT,
                    ORGANIZATION TEXT,
                    NAME TEXT,
                    GENDER TEXT,
                    SPECIALITY TEXT,
                    ADDRESS TEXT,
                    CITY TEXT,
                    STATE TEXT,
                    ZIP BIGINT,
                    LAT DECIMAL(38,15),
                    LON DECIMAL(38,14),
                    ENCOUNTERS BIGINT,
                    PROCEDURES BIGINT
                )
            """,
            'supplies': """
                CREATE TABLE IF NOT EXISTS supplies (
                    DATE DATE,
                    PATIENT TEXT,
                    ENCOUNTER TEXT,
                    CODE BIGINT,
                    DESCRIPTION TEXT,
                    QUANTITY BIGINT
                )
            """
        }

        cursor = self.connection.cursor()
        try:
            for table_name, create_sql in tables_sql.items():
                print(f"📋 Creating table: {table_name}")
                cursor.execute(create_sql)
            self.connection.commit()
            print("✅ All tables created successfully")
        except Error as e:
            print(f"❌ Error creating tables: {e}")
            raise
        finally:
            cursor.close()

    def clear_all_data(self):
        """Clear all existing data from the database"""
        tables = [
            'allergies', 'careplans', 'claims', 'claims_transactions', 'conditions',
            'devices', 'encounters', 'imaging_studies', 'immunizations', 'medications',
            'observations', 'organizations', 'patients', 'payers', 'payer_transitions',
            'procedures', 'providers', 'supplies'
        ]

        cursor = self.connection.cursor()
        try:
            for table in tables:
                cursor.execute(f"DELETE FROM {table}")
                print(f"🗑️ Cleared data from table: {table}")
            self.connection.commit()
            print("✅ All existing data cleared from database")
        except Error as e:
            print(f"❌ Failed to clear data: {e}")
            raise
        finally:
            cursor.close()

    def generate_patient_id(self, index: int) -> str:
        """Generate a unique patient ID"""
        return f"{index:03d}"

    def generate_patient(self, patient_id: str) -> Dict[str, Any]:
        """Generate a synthetic patient record for MySQL"""
        gender = random.choice(['M', 'F'])
        birth_date = self.fake.date_of_birth(minimum_age=18, maximum_age=90)

        # Generate realistic names based on gender
        if gender == 'M':
            first_name = self.fake.first_name_male()
        else:
            first_name = self.fake.first_name_female()

        last_name = self.fake.last_name()

        # Generate address
        address = self.fake.street_address()
        city = self.fake.city()
        state = self.fake.state_abbr()
        zip_code = int(self.fake.zipcode())

        # Generate SSN-like number
        ssn = self.fake.ssn().replace('-', '')

        # Generate driver's license
        drivers_license = self.fake.license_plate()

        # Generate passport
        passport = f"P{self.fake.random_number(digits=8)}"

        # Marital status
        marital_options = ['S', 'M', 'D', 'W']  # Single, Married, Divorced, Widowed
        marital = random.choice(marital_options)

        # Race and ethnicity
        race_options = ['white', 'black', 'asian', 'hispanic', 'other']
        race = random.choice(race_options)
        ethnicity = 'hispanic' if race == 'hispanic' else 'non-hispanic'

        # Healthcare expenses and coverage
        healthcare_expenses = round(random.uniform(1000, 50000), 2)
        healthcare_coverage = round(random.uniform(500, healthcare_expenses), 2)
        income = random.randint(20000, 150000)

        patient = {
            'ID': patient_id,
            'BIRTHDATE': birth_date,
            'DEATHDATE': None,  # Most patients are alive
            'SSN': ssn,
            'DRIVERS': drivers_license,
            'PASSPORT': passport,
            'PREFIX': None,
            'FIRST': first_name,
            'MIDDLE': None,
            'LAST': last_name,
            'SUFFIX': None,
            'MAIDEN': None,
            'MARITAL': marital,
            'RACE': race,
            'ETHNICITY': ethnicity,
            'GENDER': gender,
            'BIRTHPLACE': self.fake.city(),
            'ADDRESS': address,
            'CITY': city,
            'STATE': state,
            'COUNTY': self.fake.city(),  # Using city as county since Faker doesn't have county
            'FIPS': random.randint(1000, 99999),
            'ZIP': zip_code,
            'LAT': round(self.fake.latitude(), 15),
            'LON': round(self.fake.longitude(), 14),
            'HEALTHCARE_EXPENSES': healthcare_expenses,
            'HEALTHCARE_COVERAGE': healthcare_coverage,
            'INCOME': income
        }

        return patient

    def generate_condition(self, patient_id: str) -> Dict[str, Any]:
        """Generate a synthetic condition record for MySQL"""
        condition_codes = list(self.snomed_codes['conditions'].items())
        code, description = random.choice(condition_codes)

        # Generate dates
        registered_at = self.fake.date_between(start_date='-2y', end_date='today')
        stop = None  # Most conditions are ongoing
        if random.random() < 0.3:  # 30% chance of resolved condition
            stop = self.fake.date_between(start_date=registered_at, end_date='today')

        condition = {
            'REGISTERED_AT': registered_at,
            'STOP': stop,
            'PATIENT': patient_id,
            'ENCOUNTER': f"enc-{patient_id}-1",  # Link to first encounter
            'SYSTEM': 'http://snomed.info/sct',
            'CODE': int(code),
            'DESCRIPTION': description
        }

        return condition

    def generate_medication(self, patient_id: str) -> Dict[str, Any]:
        """Generate a synthetic medication record for MySQL"""
        medication_codes = list(self.rxnorm_codes.items())
        code, description = random.choice(medication_codes)

        # Generate dates
        registered_at = self.fake.date_time_between(start_date='-1y', end_date='now')
        stop = None
        if random.random() < 0.4:  # 40% chance medication is stopped
            stop = self.fake.date_time_between(start_date=registered_at, end_date='now')

        # Generate costs
        base_cost = round(random.uniform(5, 500), 2)
        payer_coverage = round(random.uniform(0, base_cost), 2)
        dispenses = random.randint(1, 12)
        total_cost = base_cost * dispenses

        medication = {
            'REGISTERED_AT': registered_at,
            'STOP': stop,
            'PATIENT': patient_id,
            'PAYER': f"payer-{random.randint(1, 10)}",
            'ENCOUNTER': f"enc-{patient_id}-1",
            'CODE': int(code),
            'DESCRIPTION': description,
            'BASE_COST': base_cost,
            'PAYER_COVERAGE': payer_coverage,
            'DISPENSES': dispenses,
            'TOTALCOST': total_cost,
            'REASONCODE': random.choice(list(self.snomed_codes['conditions'].keys())),
            'REASONDESCRIPTION': random.choice(list(self.snomed_codes['conditions'].values()))
        }

        return medication

    def generate_encounter(self, patient_id: str, encounter_num: int) -> Dict[str, Any]:
        """Generate a synthetic encounter record for MySQL"""
        encounter_codes = list(self.snomed_codes['encounters'].items())
        code, description = random.choice(encounter_codes)

        # Generate timestamps
        registered_at = self.fake.date_time_between(start_date='-1y', end_date='now')
        duration_hours = random.randint(15, 120)
        stop = registered_at + timedelta(minutes=duration_hours)

        # Generate costs
        base_cost = round(random.uniform(50, 500), 2)
        payer_coverage = round(random.uniform(0, base_cost), 2)
        total_claim_cost = base_cost

        encounter = {
            'ID': f"enc-{patient_id}-{encounter_num}",
            'REGISTERED_AT': registered_at,
            'STOP': stop,
            'PATIENT': patient_id,
            'ORGANIZATION': f"org-{random.randint(1, 10)}",
            'PROVIDER': f"prov-{random.randint(1, 50)}",
            'PAYER': f"payer-{random.randint(1, 10)}",
            'ENCOUNTERCLASS': 'ambulatory',
            'CODE': int(code),
            'DESCRIPTION': description,
            'BASE_ENCOUNTER_COST': base_cost,
            'TOTAL_CLAIM_COST': total_claim_cost,
            'PAYER_COVERAGE': payer_coverage,
            'REASONCODE': random.choice(list(self.snomed_codes['conditions'].keys())),
            'REASONDESCRIPTION': random.choice(list(self.snomed_codes['conditions'].values()))
        }

        return encounter

    def generate_allergy(self, patient_id: str) -> Dict[str, Any]:
        """Generate a synthetic allergy record for MySQL"""
        allergy_types = ['food', 'medication', 'environmental']
        allergy_type = random.choice(allergy_types)

        if allergy_type == 'food':
            code = random.randint(100000, 999999)
            description = random.choice(['Peanuts', 'Shellfish', 'Milk', 'Eggs', 'Wheat'])
            system = 'http://snomed.info/sct'
        elif allergy_type == 'medication':
            code = random.randint(100000, 999999)
            description = random.choice(['Penicillin', 'Aspirin', 'Ibuprofen', 'Codeine', 'Sulfa'])
            system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
        else:
            code = random.randint(100000, 999999)
            description = random.choice(['Dust mites', 'Pollen', 'Pet dander', 'Mold', 'Latex'])
            system = 'http://snomed.info/sct'

        registered_at = self.fake.date_between(start_date='-5y', end_date='today')

        allergy = {
            'REGISTERED_AT': registered_at,
            'STOP': None,
            'PATIENT': patient_id,
            'ENCOUNTER': f"enc-{patient_id}-1",
            'CODE': code,
            'SYSTEM': system,
            'DESCRIPTION': description,
            'TYPE': allergy_type,
            'CATEGORY': 'allergy',
            'REACTION1': random.randint(100000, 999999),
            'DESCRIPTION1': random.choice(['Rash', 'Hives', 'Itching', 'Swelling', 'Difficulty breathing']),
            'SEVERITY1': random.choice(['mild', 'moderate', 'severe']),
            'REACTION2': None,
            'DESCRIPTION2': None,
            'SEVERITY2': None
        }

        return allergy

    def generate_careplan(self, patient_id: str, careplan_num: int) -> Dict[str, Any]:
        """Generate a synthetic careplan record for MySQL"""
        careplan_types = [
            (385691007, 'Diabetes self management plan'),
            (412776001, 'Chronic obstructive pulmonary disease clinical management plan'),
            (736366004, 'Heart failure self management plan'),
            (736377007, 'Asthma clinical management plan')
        ]

        code, description = random.choice(careplan_types)

        registered_at = self.fake.date_between(start_date='-1y', end_date='today')
        stop = None
        if random.random() < 0.2:  # 20% chance careplan is completed
            stop = self.fake.date_between(start_date=registered_at, end_date='today')

        careplan = {
            'ID': f"cp-{patient_id}-{careplan_num}",
            'REGISTERED_AT': registered_at,
            'STOP': stop,
            'PATIENT': patient_id,
            'ENCOUNTER': f"enc-{patient_id}-1",
            'CODE': code,
            'DESCRIPTION': description,
            'REASONCODE': random.choice(list(self.snomed_codes['conditions'].keys())),
            'REASONDESCRIPTION': random.choice(list(self.snomed_codes['conditions'].values()))
        }

        return careplan

    def generate_all_data(self, num_patients: int = 100):
        """
        Generate comprehensive synthetic medical data

        Args:
            num_patients: Number of patients to generate
        """
        print(f"🚀 Starting generation of {num_patients} patients with associated medical data...")

        # Data containers
        patients = []
        conditions = []
        medications = []
        encounters = []
        allergies = []
        careplans = []

        # Generate patients and associated data
        print("📝 Generating patients and medical records...")
        for i in range(1, num_patients + 1):
            patient_id = self.generate_patient_id(i)
            patient = self.generate_patient(patient_id)
            patients.append(patient)

            # Generate encounters (1-3 per patient)
            num_encounters = random.randint(1, 3)
            for j in range(1, num_encounters + 1):
                encounter = self.generate_encounter(patient_id, j)
                encounters.append(encounter)

            # Generate conditions (0-3 per patient)
            num_conditions = random.randint(0, 3)
            for j in range(num_conditions):
                condition = self.generate_condition(patient_id)
                conditions.append(condition)

            # Generate medications (0-4 per patient)
            num_medications = random.randint(0, 4)
            for j in range(num_medications):
                medication = self.generate_medication(patient_id)
                medications.append(medication)

            # Generate allergies (0-2 per patient)
            num_allergies = random.randint(0, 2)
            for j in range(num_allergies):
                allergy = self.generate_allergy(patient_id)
                allergies.append(allergy)

            # Generate careplans (0-1 per patient)
            if random.random() < 0.3:  # 30% of patients have careplans
                careplan = self.generate_careplan(patient_id, 1)
                careplans.append(careplan)

        # Insert data into MySQL
        print("💾 Inserting data into MySQL...")

        self.insert_patients(patients)
        self.insert_encounters(encounters)
        self.insert_conditions(conditions)
        self.insert_medications(medications)
        self.insert_allergies(allergies)
        self.insert_careplans(careplans)

        print("🎉 Synthetic medical data generation completed!")
        print(f"📊 Summary:")
        print(f"   • Patients: {len(patients)}")
        print(f"   • Encounters: {len(encounters)}")
        print(f"   • Conditions: {len(conditions)}")
        print(f"   • Medications: {len(medications)}")
        print(f"   • Allergies: {len(allergies)}")
        print(f"   • Careplans: {len(careplans)}")

    def insert_patients(self, patients: List[Dict[str, Any]]):
        """Insert patients into MySQL"""
        if not patients:
            return

        sql = """
            INSERT INTO patients (
                ID, BIRTHDATE, DEATHDATE, SSN, DRIVERS, PASSPORT, PREFIX, FIRST, MIDDLE, LAST,
                SUFFIX, MAIDEN, MARITAL, RACE, ETHNICITY, GENDER, BIRTHPLACE, ADDRESS, CITY,
                STATE, COUNTY, FIPS, ZIP, LAT, LON, HEALTHCARE_EXPENSES, HEALTHCARE_COVERAGE, INCOME
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        cursor = self.connection.cursor()
        try:
            values = [(
                p['ID'], p['BIRTHDATE'], p['DEATHDATE'], p['SSN'], p['DRIVERS'], p['PASSPORT'],
                p['PREFIX'], p['FIRST'], p['MIDDLE'], p['LAST'], p['SUFFIX'], p['MAIDEN'],
                p['MARITAL'], p['RACE'], p['ETHNICITY'], p['GENDER'], p['BIRTHPLACE'],
                p['ADDRESS'], p['CITY'], p['STATE'], p['COUNTY'], p['FIPS'], p['ZIP'],
                p['LAT'], p['LON'], p['HEALTHCARE_EXPENSES'], p['HEALTHCARE_COVERAGE'], p['INCOME']
            ) for p in patients]

            cursor.executemany(sql, values)
            self.connection.commit()
            print(f"✅ Inserted {len(patients)} patients")
        except Error as e:
            print(f"❌ Error inserting patients: {e}")
            raise
        finally:
            cursor.close()

    def insert_encounters(self, encounters: List[Dict[str, Any]]):
        """Insert encounters into MySQL"""
        if not encounters:
            return

        sql = """
            INSERT INTO encounters (
                ID, REGISTERED_AT, STOP, PATIENT, ORGANIZATION, PROVIDER, PAYER,
                ENCOUNTERCLASS, CODE, DESCRIPTION, BASE_ENCOUNTER_COST, TOTAL_CLAIM_COST,
                PAYER_COVERAGE, REASONCODE, REASONDESCRIPTION
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        cursor = self.connection.cursor()
        try:
            values = [(
                e['ID'], e['REGISTERED_AT'], e['STOP'], e['PATIENT'], e['ORGANIZATION'],
                e['PROVIDER'], e['PAYER'], e['ENCOUNTERCLASS'], e['CODE'], e['DESCRIPTION'],
                e['BASE_ENCOUNTER_COST'], e['TOTAL_CLAIM_COST'], e['PAYER_COVERAGE'],
                e['REASONCODE'], e['REASONDESCRIPTION']
            ) for e in encounters]

            cursor.executemany(sql, values)
            self.connection.commit()
            print(f"✅ Inserted {len(encounters)} encounters")
        except Error as e:
            print(f"❌ Error inserting encounters: {e}")
            raise
        finally:
            cursor.close()

    def insert_conditions(self, conditions: List[Dict[str, Any]]):
        """Insert conditions into MySQL"""
        if not conditions:
            return

        sql = """
            INSERT INTO conditions (
                REGISTERED_AT, STOP, PATIENT, ENCOUNTER, SYSTEM, CODE, DESCRIPTION
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cursor = self.connection.cursor()
        try:
            values = [(
                c['REGISTERED_AT'], c['STOP'], c['PATIENT'], c['ENCOUNTER'],
                c['SYSTEM'], c['CODE'], c['DESCRIPTION']
            ) for c in conditions]

            cursor.executemany(sql, values)
            self.connection.commit()
            print(f"✅ Inserted {len(conditions)} conditions")
        except Error as e:
            print(f"❌ Error inserting conditions: {e}")
            raise
        finally:
            cursor.close()

    def insert_medications(self, medications: List[Dict[str, Any]]):
        """Insert medications into MySQL"""
        if not medications:
            return

        sql = """
            INSERT INTO medications (
                REGISTERED_AT, STOP, PATIENT, PAYER, ENCOUNTER, CODE, DESCRIPTION,
                BASE_COST, PAYER_COVERAGE, DISPENSES, TOTALCOST, REASONCODE, REASONDESCRIPTION
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        cursor = self.connection.cursor()
        try:
            values = [(
                m['REGISTERED_AT'], m['STOP'], m['PATIENT'], m['PAYER'], m['ENCOUNTER'],
                m['CODE'], m['DESCRIPTION'], m['BASE_COST'], m['PAYER_COVERAGE'],
                m['DISPENSES'], m['TOTALCOST'], m['REASONCODE'], m['REASONDESCRIPTION']
            ) for m in medications]

            cursor.executemany(sql, values)
            self.connection.commit()
            print(f"✅ Inserted {len(medications)} medications")
        except Error as e:
            print(f"❌ Error inserting medications: {e}")
            raise
        finally:
            cursor.close()

    def insert_allergies(self, allergies: List[Dict[str, Any]]):
        """Insert allergies into MySQL"""
        if not allergies:
            return

        sql = """
            INSERT INTO allergies (
                REGISTERED_AT, STOP, PATIENT, ENCOUNTER, CODE, SYSTEM, DESCRIPTION,
                TYPE, CATEGORY, REACTION1, DESCRIPTION1, SEVERITY1, REACTION2, DESCRIPTION2, SEVERITY2
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        cursor = self.connection.cursor()
        try:
            values = [(
                a['REGISTERED_AT'], a['STOP'], a['PATIENT'], a['ENCOUNTER'], a['CODE'],
                a['SYSTEM'], a['DESCRIPTION'], a['TYPE'], a['CATEGORY'], a['REACTION1'],
                a['DESCRIPTION1'], a['SEVERITY1'], a['REACTION2'], a['DESCRIPTION2'], a['SEVERITY2']
            ) for a in allergies]

            cursor.executemany(sql, values)
            self.connection.commit()
            print(f"✅ Inserted {len(allergies)} allergies")
        except Error as e:
            print(f"❌ Error inserting allergies: {e}")
            raise
        finally:
            cursor.close()

    def insert_careplans(self, careplans: List[Dict[str, Any]]):
        """Insert careplans into MySQL"""
        if not careplans:
            return

        sql = """
            INSERT INTO careplans (
                ID, REGISTERED_AT, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION,
                REASONCODE, REASONDESCRIPTION
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        cursor = self.connection.cursor()
        try:
            values = [(
                c['ID'], c['REGISTERED_AT'], c['STOP'], c['PATIENT'], c['ENCOUNTER'],
                c['CODE'], c['DESCRIPTION'], c['REASONCODE'], c['REASONDESCRIPTION']
            ) for c in careplans]

            cursor.executemany(sql, values)
            self.connection.commit()
            print(f"✅ Inserted {len(careplans)} careplans")
        except Error as e:
            print(f"❌ Error inserting careplans: {e}")
            raise
        finally:
            cursor.close()

    def run(self, num_patients: int = 100):
        """Main execution method"""
        try:
            print("🔌 Connecting to MySQL...")
            self.connect_to_mysql()

            print("📋 Creating tables...")
            self.create_tables()

            print("🗑️ Clearing existing data...")
            self.clear_all_data()

            print("🏥 Generating synthetic medical data...")
            self.generate_all_data(num_patients)

            print("✅ All operations completed successfully!")

        except Exception as e:
            print(f"❌ Error during execution: {e}")
            raise
        finally:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                print("🔌 MySQL connection closed")

def main():
    """Main function with improved configuration"""
    import argparse

    parser = argparse.ArgumentParser(description='Generate synthetic medical data for MySQL')
    parser.add_argument('--patients', type=int, help='Number of patients to generate')
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')

    args = parser.parse_args()

    print("🏥 MySQL Synthetic Medical Data Generator")
    print("=" * 50)

    # Configuration - Try environment variables first, then defaults
    MYSQL_CONFIG = {
        'host': os.getenv('MYSQL_HOST', 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com'),
        'port': int(os.getenv('MYSQL_PORT', 4000)),
        'user': os.getenv('MYSQL_USER', 'dXbMX82BkM75qwH.root'),
        'password': os.getenv('MYSQL_PASSWORD', 'j7xHZpcsVvjjMZav'),
        'database': os.getenv('MYSQL_DATABASE', 'test')
    }

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
            NUM_PATIENTS = 100  # Default

    print(f"\n⚙️  Configuration:")
    print(f"   • MySQL Host: {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}")
    print(f"   • Database: {MYSQL_CONFIG['database']}")
    print(f"   • User: {MYSQL_CONFIG['user']}")
    print(f"   • Patients to generate: {NUM_PATIENTS}")

    if not args.force:
        confirm = input("\n🚨 This will DELETE ALL existing data in the database. Continue? (y/N): ").strip().lower()
        if confirm not in ['y', 'yes']:
            print("❌ Operation cancelled by user")
            return

    # Create and run the generator
    try:
        generator = MySQLDataGenerator(mysql_config=MYSQL_CONFIG)
        generator.run(num_patients=NUM_PATIENTS)
        print("\n🎉 Synthetic medical data generation completed successfully!")

        # Print query examples
        print("\n🔍 Query Examples:")
        print("# Find patient by ID:")
        print("SELECT * FROM patients WHERE ID = '001';")
        print("# Find all conditions for a patient:")
        print("SELECT * FROM conditions WHERE PATIENT = '001';")
        print("# Find all medications for a patient:")
        print("SELECT * FROM medications WHERE PATIENT = '001';")
        print("# Find all encounters for a patient:")
        print("SELECT * FROM encounters WHERE PATIENT = '001';")

    except Exception as e:
        print(f"\n❌ Error during execution: {e}")
        print("\n🔧 Troubleshooting:")
        print("   • Check your MySQL connection string and credentials")
        print("   • Ensure the MySQL server is running and accessible")
        print("   • Verify database permissions for the user")
        print("   • Check network connectivity and firewall settings")


if __name__ == "__main__":
    main()