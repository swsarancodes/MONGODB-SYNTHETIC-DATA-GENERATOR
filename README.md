# Synthetic FHIR Medical Data Generator

This project generates comprehensive synthetic medical data using FHIR (Fast Healthcare Interoperability Resources) standards. The generated data includes patients, observations, conditions, medication requests, and encounters - all compliant with HL7 FHIR R4 specifications.

## Features

- 🏥 **FHIR R4 Compliant**: All generated data follows HL7 FHIR R4 standards
- 🔬 **Comprehensive Medical Data**: Patients, vitals, lab results, diagnoses, medications, and encounters
- 🎯 **Realistic Data**: Uses Faker library to generate realistic patient demographics and medical information
- 🗄️ **MongoDB Integration**: Direct insertion into MongoDB collections
- 🔧 **Open Source**: Built with open source Python libraries
- ⚡ **Configurable**: Easily adjust the number of patients and data volume

## Prerequisites

- Python 3.8 or higher
- MongoDB instance (local or cloud)
- Internet connection for package installation

## Quick FHIR Data Insertion

A simple script to insert sample FHIR R4 data with patientId linking:

```bash
# Install dependencies
uv venv
.venv\Scripts\activate
uv pip sync requirements.txt

# Copy config
copy config.env .env

# Run the inserter
python insert_fhir_data.py
```

This creates sample Patient, Observation, Condition, and Encounter resources all linked by `patientId: "patient-001"`.

## Features

- 🏥 **FHIR R4 Compliant**: Sample data follows HL7 FHIR R4 standards
- 🔗 **Patient Linking**: All resources linked via patientId field
- 🗄️ **MongoDB Ready**: Direct insertion with proper structure
- 📊 **Verification**: Built-in queries to verify linking works

### Option 2: Environment Variables
```bash
# 1. Copy and edit configuration
cp config.env .env
# Edit .env with your MongoDB settings

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the generator
python mongodb.py
```

### Option 3: Local MongoDB Only
```bash
# 1. Install MongoDB locally
# Windows: https://docs.mongodb.com/manual/tutorial/install-mongodb-on-windows/
# macOS: brew install mongodb-community
# Linux: Follow your distro's package manager

# 2. Start MongoDB service
mongod

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Run the generator (choose option 2 for local)
python mongodb.py
```

## Troubleshooting

### DNS/Network Errors
```
The DNS query name does not exist: _mongodb._tcp.cluster0...
```
**Solutions:**
- ✅ **Use local MongoDB**: Choose option 2 when prompted
- ✅ **Check Atlas cluster**: Ensure your MongoDB Atlas cluster exists and is running
- ✅ **Update connection string**: Verify your Atlas connection string is correct
- ✅ **Network access**: Ensure your network allows access to MongoDB Atlas

### Connection Timeouts
```
ServerSelectionTimeoutError: No servers found...
```
**Solutions:**
- ✅ **Check MongoDB service**: Ensure local MongoDB is running (`mongod`)
- ✅ **Firewall settings**: Allow MongoDB ports (27017)
- ✅ **Atlas IP whitelist**: Add your IP to Atlas network access
- ✅ **Connection string**: Verify username/password in Atlas URI

### Import Errors
```
Import "faker" could not be resolved
```
**Solutions:**
- ✅ **Install dependencies**: `pip install -r requirements.txt`
- ✅ **Virtual environment**: Use `python -m venv venv` and activate it
- ✅ **Python version**: Ensure Python 3.8+ is installed

### Permission Errors
```
Authentication failed
```
**Solutions:**
- ✅ **Atlas credentials**: Verify username/password in connection string
- ✅ **Database user**: Ensure user has read/write permissions
- ✅ **IP whitelist**: Add your IP address to Atlas network access

## Configuration

### MongoDB Connection
Update the `MONGO_URI` variable in `mongodb.py`:
```python
MONGO_URI = "mongodb+srv://username:password@cluster.mongodb.net/database"
```

### Number of Patients
Modify the `NUM_PATIENTS` variable in `mongodb.py`:
```python
NUM_PATIENTS = 500  # Generate 500 patients instead of default 300
```

## Generated Data Structure

### Patients Collection
- **Resource Type**: Patient
- **Fields**: Demographics, identifiers, addresses, telecom, marital status
- **Sample ID Format**: pat-001, pat-002, etc.

### Observations Collection
- **Resource Type**: Observation
- **Types**: Vital signs (BP, HR, BMI, weight, height) and lab results
- **Linked to**: Patient resources

### Conditions Collection
- **Resource Type**: Condition
- **Types**: Various medical diagnoses (diabetes, hypertension, asthma, etc.)
- **Linked to**: Patient resources

### Medication Requests Collection
- **Resource Type**: MedicationRequest
- **Types**: Prescriptions with dosages and timing
- **Linked to**: Patient and Encounter resources

### Encounters Collection
- **Resource Type**: Encounter
- **Types**: Appointments and visits
- **Linked to**: Patient resources

## FHIR Compliance

All generated data follows HL7 FHIR R4 standards:

- **Coding Systems Used**:
  - SNOMED CT for diagnoses and procedures
  - LOINC for lab tests and observations
  - RxNorm for medications
  - HL7 v3 for marital status and encounter classes

- **Resource Relationships**:
  - Observations reference Patients
  - Conditions reference Patients
  - MedicationRequests reference Patients and Encounters
  - Encounters reference Patients

## Data Volume

Default generation creates:
- **300 Patients**
- **600-1500 Observations** (2-5 per patient)
- **0-900 Conditions** (0-3 per patient)
- **0-1200 Medication Requests** (0-4 per patient)
- **300-900 Encounters** (1-3 per patient)

## Dependencies

- `faker>=15.0.0`: Generate realistic fake data
- `pymongo>=4.0.0`: MongoDB driver for Python
- `python-dotenv>=1.0.0`: Environment variable management
- `requests>=2.25.0`: HTTP library (for future extensions)

## Security Notes

- The script will **clear all existing data** in your MongoDB database
- Make sure to backup important data before running
- Use environment variables for sensitive connection strings
- Generated data is synthetic and should not be used for real medical purposes

## Customization

### Adding New Medical Conditions
Add to the `snomed_codes['conditions']` dictionary in `mongodb.py`:
```python
'123456789': 'New Medical Condition'
```

### Adding New Medications
Add to the `rxnorm_codes` dictionary:
```python
'987654321': 'New Medication 100 MG Oral Tablet'
```

### Modifying Data Distributions
Adjust the random generation logic in the respective methods to change:
- Age distributions
- Gender ratios
- Condition prevalence
- Medication frequencies

## Troubleshooting

### Connection Issues
- Verify your MongoDB connection string
- Check network connectivity
- Ensure MongoDB instance is running and accessible

### Import Errors
- Run `pip install -r requirements.txt`
- Ensure Python 3.8+ is installed
- Check for conflicting package versions

### Memory Issues
- Reduce `NUM_PATIENTS` for large datasets
- Process data in smaller batches if needed

## License

This project is open source and available under the MIT License.

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve the synthetic data generation.

---

# FHIR Resource Ingestor

This component provides a robust FHIR R4 resource ingestion system that can process FHIR resources from JSON files or HTTP endpoints and store them in MongoDB with proper patient linking.

## Features

- 📁 **File Input**: Process directories containing FHIR JSON files
- 🌐 **HTTP API**: RESTful endpoint for real-time resource ingestion
- 🔗 **Patient Linking**: Automatic patientId derivation and interlinking
- ✅ **Validation**: Basic FHIR R4 constraint validation
- 🗄️ **MongoDB Optimized**: Upsert operations with proper indexing
- 📊 **Error Handling**: Dead letter queue for failed resources
- 📈 **Batch Processing**: Efficient bulk operations for large datasets

## Supported Resource Types

- Patient
- Observation
- Encounter
- Condition
- Procedure
- MedicationRequest
- DocumentReference
- AllergyIntolerance
- DiagnosticReport

## Setup

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Configuration
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your MongoDB settings
# MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
# DB_NAME=fhir_db
```

## Usage

### File Mode
Process all JSON files in a directory:
```bash
python fhir_ingestor.py --input-dir ./fhir_data
```

### HTTP Server Mode
Start a REST API server:
```bash
python fhir_ingestor.py --http --port 5000
```

Send FHIR resources via POST:
```bash
curl -X POST http://localhost:5000/ingest \
  -H "Content-Type: application/json" \
  -d @fhir_resources.json
```

### Health Check
```bash
curl http://localhost:5000/health
```

## Data Model

### Collections
Each resource type gets its own collection:
- `patients`
- `observations`
- `encounters`
- `conditions`
- `procedures`
- `medication_requests`
- `document_references`
- `allergies`
- `diagnostic_reports`
- `dead_fhir` (for failed resources)

### Document Structure
```json
{
  "resourceType": "Patient",
  "id": "pat-001",
  "patientId": "pat-001",
  "resource": { /* full FHIR resource */ },
  "meta.versionId": "1",
  "meta.lastUpdated": "2023-01-01T00:00:00Z",
  "status": "active",
  "code": { /* FHIR code */ },
  "effectiveDateTime": "2023-01-01T00:00:00Z",
  "encounterId": "enc-001"
}
```

## Patient Linking Logic

The system derives `patientId` using this priority order:

1. `subject.reference` (e.g., "Patient/pat-001")
2. `patient.reference` (e.g., "Patient/pat-001")
3. `encounter.subject.reference` (for Encounter resources)
4. Contained Patient resources

All references are normalized to "Patient/{id}" format.

## Query Examples

### Find All Resources for a Patient
```javascript
// Get patient details
db.patients.findOne({ id: "pat-001" })

// Get all observations for a patient
db.observations.find({ patientId: "pat-001" })

// Get all conditions for a patient
db.conditions.find({ patientId: "pat-001" })
```

### Join-like Queries
```javascript
// Get patient with their recent observations
patient = db.patients.findOne({ id: "pat-001" })
observations = db.observations.find({
  patientId: patient.id,
  effectiveDateTime: { $gte: "2023-01-01" }
}).toArray()
```

### Aggregation Examples
```javascript
// Count resources per patient
db.observations.aggregate([
  { $group: { _id: "$patientId", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])

// Find patients with specific conditions
db.conditions.aggregate([
  { $match: { "code.coding.code": "38341003" } }, // Hypertension
  { $lookup: {
      from: "patients",
      localField: "patientId",
      foreignField: "id",
      as: "patient"
    }
  }
])
```

## Extending to New Resource Types

1. Add to `SUPPORTED_RESOURCES` dict in `fhir_ingestor.py`:
```python
SUPPORTED_RESOURCES = {
    # ... existing resources
    'Immunization': 'immunizations',
    'CarePlan': 'care_plans'
}
```

2. Add validation logic in `validate_resource()`:
```python
elif resource_type == 'Immunization':
    if not resource.get('vaccineCode'):
        errors.append("Immunization missing vaccineCode")
```

3. Update patient derivation if needed for the new resource type.

## Error Handling

### Dead Letter Queue
Failed resources are stored in `dead_fhir` collection with:
- Original resource
- Failure reason
- Error details
- Timestamp

### Common Issues
- **Missing patient reference**: Resource cannot be linked to a patient
- **Validation errors**: Missing required FHIR fields
- **Storage errors**: MongoDB connection or permission issues

## Performance

- **Batch Size**: Default 500 resources per batch
- **Indexes**: Automatic creation of patientId and date indexes
- **Upsert**: Efficient updates for existing resources
- **Connection**: Retryable writes with 10s timeouts

## API Reference

### POST /ingest
Ingest an array of FHIR resources.

**Request:**
```json
[
  {
    "resourceType": "Patient",
    "id": "pat-001",
    "name": [{"family": "Doe", "given": ["John"]}],
    "birthDate": "1980-01-01"
  }
]
```

**Response:**
```json
{
  "status": "success",
  "processed": 1,
  "inserted": 1,
  "updated": 0,
  "errors": 0
}
```

### GET /health
Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2023-01-01T00:00:00"
}
```