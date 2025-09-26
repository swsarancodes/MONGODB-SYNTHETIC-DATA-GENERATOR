# MySQL Synthetic Medical Data Generator

This project generates comprehensive synthetic medical data according to the provided Snowflake schema. The generated data includes patients, conditions, medications, encounters, allergies, and careplans - all structured according to the specified table schemas.

## Features

- 🏥 **Schema Compliant**: All generated data follows the provided Snowflake schema structure
- 🔬 **Comprehensive Medical Data**: Patients, conditions, medications, encounters, allergies, and careplans
- 🎯 **Realistic Data**: Uses Faker library to generate realistic patient demographics and medical information
- 🗄️ **MySQL Integration**: Direct insertion into MySQL tables with proper data types
- 🔧 **Open Source**: Built with open source Python libraries
- ⚡ **Configurable**: Easily adjust the number of patients and data volume

## Prerequisites

- Python 3.8 or higher
- MySQL instance (local or cloud like TiDB Cloud)
- Internet connection for package installation

## Quick Setup

```bash
# Install dependencies using uv (fast Python package manager)
uv pip sync requirements.txt

# Copy config
cp .env.example .env

# Edit .env with your MySQL settings
# MYSQL_HOST=gateway01.ap-southeast-1.prod.aws.tidbcloud.com
# MYSQL_PORT=4000
# MYSQL_USER=your_username
# MYSQL_PASSWORD=your_password
# MYSQL_DATABASE=test

# Run the generator
uv run mysql_data_generator.py
```

## Installation Options

### Option 1: TiDB Cloud (Recommended)
```bash
# 1. Sign up for TiDB Cloud at https://tidbcloud.com
# 2. Create a cluster and get connection details
# 3. Update .env with your TiDB connection string
# 4. Install dependencies
uv pip sync requirements.txt
# 5. Run the generator
uv run mysql_data_generator.py
```

### Option 2: Environment Variables
```bash
# 1. Copy and edit configuration
cp .env.example .env
# Edit .env with your MySQL settings

# 2. Install dependencies
uv pip sync requirements.txt

# 3. Run the generator
uv run mysql_data_generator.py
```

### Option 3: Local MySQL Only
```bash
# 1. Install MySQL locally
# Windows: https://dev.mysql.com/downloads/mysql/
# macOS: brew install mysql
# Linux: Follow your distro's package manager

# 2. Start MySQL service
# Windows: net start mysql
# macOS/Linux: sudo systemctl start mysql

# 3. Install Python dependencies
uv pip sync requirements.txt

# 4. Run the generator
uv run mysql_data_generator.py
```

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
uv pip sync requirements.txt

# 4. Run the generator (choose option 2 for local)
uv run mongodb.py
```

## Troubleshooting

### DNS/Network Errors
```
Can't connect to MySQL server on 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000'
```
**Solutions:**
- ✅ **Check TiDB Cloud cluster**: Ensure your TiDB Cloud cluster exists and is running
- ✅ **Update connection details**: Verify your TiDB connection parameters in .env
- ✅ **Network access**: Ensure your network allows access to TiDB Cloud
- ✅ **Use local MySQL**: Switch to local MySQL if cloud access fails

### Connection Timeouts
```
Connection timed out or 2003: Can't connect to MySQL server
```
**Solutions:**
- ✅ **Check MySQL service**: Ensure local MySQL is running
- ✅ **Firewall settings**: Allow MySQL ports (3306 or 4000 for TiDB)
- ✅ **TiDB IP whitelist**: Add your IP to TiDB Cloud network access
- ✅ **Connection parameters**: Verify host, port, username, password, and database name

### Import Errors
```
Import "faker" could not be resolved
```
**Solutions:**
- ✅ **Install dependencies**: `uv pip sync requirements.txt`
- ✅ **Virtual environment**: Use `python -m venv venv` and activate it
- ✅ **Python version**: Ensure Python 3.8+ is installed

### Authentication Errors
```
Access denied for user
```
**Solutions:**
- ✅ **TiDB credentials**: Verify username/password in .env
- ✅ **Database permissions**: Ensure user has CREATE, INSERT, DELETE permissions
- ✅ **IP whitelist**: Add your IP address to TiDB Cloud network access

## Configuration

### MySQL Connection
Update the MySQL connection variables in `.env`:
```bash
MYSQL_HOST=gateway01.ap-southeast-1.prod.aws.tidbcloud.com
MYSQL_PORT=4000
MYSQL_USER=your_username
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=test
```

### Number of Patients
Modify the `NUM_PATIENTS` variable in `.env` or use command line:
```bash
# Via environment variable
NUM_PATIENTS=500 uv run mysql_data_generator.py

# Via command line argument
uv run mysql_data_generator.py --patients 500
```

## Generated Data Structure

### Patients Table
- **Fields**: ID, demographics (name, birthdate, gender), addresses, healthcare expenses
- **Sample ID Format**: 001, 002, etc.
- **Links to**: All other tables via PATIENT field

### Conditions Table
- **Fields**: Medical diagnoses with SNOMED codes
- **Linked to**: Patients via PATIENT field
- **Types**: Various medical conditions (diabetes, hypertension, asthma, etc.)

### Medications Table
- **Fields**: Prescriptions with RxNorm codes, costs, and dispensing info
- **Linked to**: Patients and encounters
- **Types**: Various medications with realistic dosages

### Encounters Table
- **Fields**: Healthcare visits with providers, costs, and reasons
- **Linked to**: Patients via PATIENT field
- **Types**: Various encounter types (check-ups, symptom visits, etc.)

### Allergies Table
- **Fields**: Allergic reactions with severity levels
- **Linked to**: Patients via PATIENT field
- **Types**: Food, medication, and environmental allergies

### Careplans Table
- **Fields**: Care management plans for chronic conditions
- **Linked to**: Patients via PATIENT field
- **Types**: Diabetes management, heart failure plans, etc.

### Encounters Collection
- **Resource Type**: Encounter
- **Types**: Appointments and visits
- **Linked to**: Patient resources

## Schema Compliance

All generated data follows the provided Snowflake schema structure:

- **Coding Systems Used**:
  - SNOMED CT for diagnoses and procedures
  - RxNorm for medications

- **Table Relationships**:
  - Conditions reference Patients via PATIENT field
  - Medications reference Patients and Encounters
  - Encounters reference Patients
  - Allergies reference Patients
  - Careplans reference Patients

## Data Volume

Default generation creates:
- **100 Patients**
- **0-300 Conditions** (0-3 per patient)
- **0-400 Medications** (0-4 per patient)
- **100-300 Encounters** (1-3 per patient)
- **0-200 Allergies** (0-2 per patient)
- **0-30 Careplans** (0-1 per patient, 30% probability)

## Dependencies

- `faker>=15.0.0`: Generate realistic fake data
- `mysql-connector-python>=8.0.0`: MySQL driver for Python
- `python-dotenv>=1.0.0`: Environment variable management
- `requests>=2.25.0`: HTTP library (for future extensions)

## Security Notes

- The script will **clear all existing data** in your MySQL database tables
- Make sure to backup important data before running
- Use environment variables for sensitive connection credentials
- Generated data is synthetic and should not be used for real medical purposes

## Customization

### Adding New Medical Conditions
Add to the `snomed_codes['conditions']` dictionary in `mysql_data_generator.py`:
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
- Verify your MySQL connection parameters in .env
- Check network connectivity
- Ensure MySQL/TiDB instance is running and accessible

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
uv run fhir_ingestor.py --input-dir ./fhir_data
```

### HTTP Server Mode
Start a REST API server:
```bash
uv run fhir_ingestor.py --http --port 5000
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