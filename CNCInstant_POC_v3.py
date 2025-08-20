import os
import json
import pandas as pd
import numpy as np
import snowflake.connector
from datetime import datetime

# Your existing config
SNOWFLAKE_CONFIG = {
    'account': 'DWNFBOS-FACTORY',
    'user': 'nick.pemsingh@protolabs.com',
    'authenticator': 'externalbrowser',
    'role': 'DATA_ENGINEER_FR',
    'warehouse': 'ADHOC_WH',
    'database': 'SANDBOX',
    'schema': 'CNC_INSTANT',
}

DATA_DIR = r"C:\Users\nick.pemsingh\Desktop\CNC_INSTANT Sample Data"
JSON_FILES = ["analysis_output_lom.json", "analysis_output_thin.json", "analysis_output_thread.json"]

def load_json_to_snowflake():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    try:
        print("Connected to Snowflake successfully!")
        
        for filename in JSON_FILES:
            file_path = os.path.join(DATA_DIR, filename)
            
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                continue
            
            print(f"Processing file: {filename}")
            
            # Read file as string
            with open(file_path, 'r', encoding='utf-8') as f:
                json_string = f.read()
            
            # Parse only to get model_ID
            json_data = json.loads(json_string)
            model_id = json_data.get('sourceDetails', {}).get('modelId')
            
            if not model_id:
                print(f"Warning: No model_ID found in {filename}")
                continue
            
            created_datetime = datetime.now()
            
            # Simple insert - let Snowflake auto-convert string to VARIANT
            cursor.execute("""
                INSERT INTO SANDBOX.CNC_INSTANT.CNC_INSTANT_JSON 
                (MODEL_ID, ENTIRE_JSON, FILENAME, CREATEDDATETIME)
                VALUES (%s, %s, %s, %s)
            """, (model_id, json_string, filename, created_datetime))
            
            print(f"✅ Inserted: {filename} with Model ID: {model_id}")
        
        conn.commit()
        print(f"\n🎉 Successfully loaded {len(JSON_FILES)} files!")
        
        cursor.execute("SELECT MODEL_ID, FILENAME, CREATEDDATETIME FROM SANDBOX.CNC_INSTANT.CNC_INSTANT_JSON ORDER BY CREATEDDATETIME DESC")
        results = cursor.fetchall()
        
        print("\n--- Verification ---")
        for row in results:
            print(f"Model ID: {row[0]} | File: {row[1]} | Created: {row[2]}")
            
    except Exception as e:
        print(f"❌ Error: {str(e)[:200]}...")
        conn.rollback()
    
    finally:
        cursor.close()
        conn.close()
        print("\nConnection closed.")

if __name__ == "__main__":
    load_json_to_snowflake()