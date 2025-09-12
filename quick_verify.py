#!/usr/bin/env python3
"""
Quick Data Verification Script
A simplified version for quick checks
"""

import os
from pymongo import MongoClient
from dotenv import load_dotenv

def quick_verify():
    """Quick verification of data insertion"""
    # Load environment variables from config.env or .env
    if os.path.exists('.env'):
        load_dotenv('.env')
    elif os.path.exists('config.env'):
        load_dotenv('config.env')
    else:
        print("❌ No configuration file found (.env or config.env)")
        return

    # Get MongoDB connection
    mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    database_name = os.getenv('DATABASE_NAME', 'medical_db')

    # Check if URI is placeholder
    if 'your-username' in mongo_uri or 'your-password' in mongo_uri:
        print("❌ Please update your MongoDB URI in config.env or .env file")
        print("   Current URI:", mongo_uri)
        return

    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        db = client[database_name]

        print("🔍 QUICK DATA VERIFICATION")
        print("=" * 40)
        print(f"📍 Database: {database_name}")
        print(f"🔗 Connection: {'Atlas' if 'mongodb+srv' in mongo_uri else 'Local'}")
        print()

        # Check collections
        collections = ['patients', 'observations', 'conditions', 'medication_requests', 'encounters']
        total_docs = 0

        for collection_name in collections:
            try:
                collection = db[collection_name]
                count = collection.count_documents({})
                total_docs += count

                # Check if FHIR format
                sample = collection.find_one({})
                is_fhir = bool(sample and 'resourceType' in sample)

                status = "✅" if count > 0 else "❌"
                format_str = " (FHIR)" if is_fhir else ""
                print(f"   {status} {collection_name}: {count:,} documents{format_str}")

            except Exception as e:
                print(f"   ❌ {collection_name}: Error - {e}")

        print(f"\n📊 TOTAL: {total_docs:,} documents across {len(collections)} collections")

        # Check for duplicate patient IDs
        patients_collection = db['patients']
        pipeline = [
            {'$unwind': '$identifier'},
            {'$group': {
                '_id': '$identifier.value',
                'count': {'$sum': 1},
                'patients': {'$push': '$_id'}
            }},
            {'$match': {'count': {'$gt': 1}}},
            {'$limit': 5}  # Show first 5 duplicates
        ]

        duplicates = list(patients_collection.aggregate(pipeline))

        if duplicates:
            print(f"   ⚠️  Found {len(duplicates)} duplicate patient IDs:")
            for dup in duplicates[:3]:  # Show first 3
                print(f"      ID '{dup['_id']}': used by {dup['count']} patients")
        else:
            print("   ✅ No duplicate patient IDs found")

        # Sample patient IDs
        print("\n📋 Sample Patient IDs:")
        sample_patients = patients_collection.find({}, {'id': 1, 'identifier': 1, 'name': 1}).limit(3)

        for patient in sample_patients:
            patient_id = patient.get('id', 'N/A')
            identifiers = patient.get('identifier', [])
            name = patient.get('name', [{}])[0].get('given', ['Unknown'])[0] + ' ' + patient.get('name', [{}])[0].get('family', 'Unknown')

            if identifiers:
                id_value = identifiers[0].get('value', 'N/A')
                print(f"   {name}: FHIR ID={patient_id}, Identifier={id_value}")
            else:
                print(f"   {name}: FHIR ID={patient_id}, No identifier")

        # Quick health check
        if total_docs >= 2000:  # Reasonable minimum
            print("🎉 SUCCESS: Data appears to be properly inserted!")
            print("   💡 Your synthetic FHIR medical data is ready to use!")
        else:
            print("⚠️  WARNING: Document count seems low")

        client.close()

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("💡 Make sure your MongoDB URI is correct in config.env or .env")