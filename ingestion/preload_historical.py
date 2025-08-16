import requests
import pandas as pd
from collections import defaultdict
import json
from datetime import datetime, timedelta
import numpy as np
import os
import re
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
QUALITY_FILE = "data/water_quality_data.csv"
FLOW_FILE = "data/water_flow_data.csv"
INGESTION_PERCENTAGE = 0.8

class SupabaseRestIngestion:
    def __init__(self, supabase_url=SUPABASE_URL, anon_key=SUPABASE_ANON_KEY):
        self.supabase_url = supabase_url
        self.anon_key = anon_key
        self.headers = {
            'apikey': self.anon_key,
            'Authorization': f'Bearer {self.anon_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        
        # Define allowed parameter names exactly as in database schema
        self.ALLOWED_PARAMETERS = {
            'HUMIDITY',
            'ETP (TDS)',
            'ETP (pH)',
            'STP (TDS)',
            'STP (TSS)',
            'STP (BOD)',
            'STP (pH)',
            'STP (COD)'
        }

    def normalize_parameter_name(self, raw_name):
        """Normalize parameter names to match database schema exactly"""
        # First clean the raw name
        cleaned = raw_name.strip()
        
        # Remove any numbering (like "1.") if present
        cleaned = re.sub(r'^\d+\.\s*', '', cleaned)
        
        # Handle cases with trailing commas - remove them
        cleaned = cleaned.rstrip(',')
        
        # Remove "Safe Range" part if present
        if 'Safe Range:' in cleaned:
            cleaned = cleaned.split('Safe Range:')[0].strip()
        
        # Remove any remaining non-alphanumeric characters except parentheses and spaces
        cleaned = re.sub(r'[^\w\s()]', '', cleaned).strip()
        
        # Specific normalization for common variations
        name_mapping = {
            'ETP TDS': 'ETP (TDS)',
            'ETP pH': 'ETP (pH)',
            'STP TDS': 'STP (TDS)',
            'STP TSS': 'STP (TSS)',
            'STP BOD': 'STP (BOD)',
            'STP pH': 'STP (pH)',
            'STP COD': 'STP (COD)',
            'HUMIDITY': 'HUMIDITY',
            'ETP(TDS)': 'ETP (TDS)',
            'ETP(pH)': 'ETP (pH)',
            'STP(TDS)': 'STP (TDS)',
            'STP(TSS)': 'STP (TSS)',
            'STP(BOD)': 'STP (BOD)',
            'STP(pH)': 'STP (pH)',
            'STP(COD)': 'STP (COD)'
        }
        
        # Check if the cleaned name matches any variations
        normalized = name_mapping.get(cleaned, cleaned)
        
        # Ensure proper spacing around parentheses
        normalized = normalized.replace('(', ' (').replace(')', ') ').strip()
        normalized = normalized.replace('  ', ' ')
        
        # Final validation against allowed parameters
        if normalized in self.ALLOWED_PARAMETERS:
            return normalized
        
        print(f"⚠️ Unrecognized parameter name: '{raw_name}' (normalized to '{normalized}')")
        return None

    def test_connection(self):
        """Test connection to Supabase REST API"""
        try:
            response = requests.get(
                f"{self.supabase_url}/rest/v1/water_quality?select=count",
                headers=self.headers,
                timeout=10
            )
            if response.status_code == 200:
                print("✅ Successfully connected to Supabase REST API")
                return True
            print(f"❌ API connection failed: {response.status_code}")
            return False
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
    
    def clear_existing_data(self):
        """Clear existing data via REST API"""
        try:
            # Clear water_quality table
            response = requests.delete(
                f"{self.supabase_url}/rest/v1/water_quality?id=gte.0",
                headers=self.headers
            )
            print(f"🧪 Water quality data cleared: {response.status_code}")
            
            # Clear flow_rate table
            response = requests.delete(
                f"{self.supabase_url}/rest/v1/flow_rate?id=gte.0",
                headers=self.headers
            )
            print(f"🌊 Water flow data cleared: {response.status_code}")
            
            print("✅ Existing data cleared via REST API")
        except Exception as e:
            print(f"❌ Error clearing data: {e}")
            raise
    
    def process_water_quality_data(self, file_path):
        """Process water quality data with strict parameter validation"""
        try:
            df = pd.read_csv(file_path, header=None)
            print(f"📊 Water Quality CSV loaded: {len(df)} rows")
            
            rows_by_param = defaultdict(list)
            current_param = None
            safe_min, safe_max = None, None
            skipped_invalid_params = 0
            skipped_null_values = 0

            for _, row in df.iterrows():
                line = str(row[0])

                # Skip empty lines
                if pd.isna(row[0]):
                    continue

                # Handle parameter definition lines (with "Safe Range")
                if "Safe Range:" in line:
                    try:
                        # Extract the full parameter definition line
                        param_line = line.split("Safe Range:")[0].strip()
                        
                        # Remove numbering if present (like "1. ")
                        param_line = re.sub(r'^\d+\.\s*', '', param_line)
                        
                        # Normalize the parameter name
                        current_param = self.normalize_parameter_name(param_line)
                        if not current_param:
                            skipped_invalid_params += 1
                            continue
                        
                        # Extract safe range values
                        safe_range_str = line.split("Safe Range:")[1].strip()
                        safe_range_str = safe_range_str.strip(" ()")
                        safe_min, safe_max = map(float, safe_range_str.split("to"))
                        continue

                    except Exception as e:
                        print(f"⚠️ Error processing parameter definition: {line} - {str(e)}")
                        skipped_invalid_params += 1
                        continue

                # Skip header lines
                if any(x in line.lower() for x in ["date", "time", "parameter"]):
                    continue

                # Process data rows
                try:
                    if not current_param:
                        continue
                        
                    # Skip rows that don't have enough columns
                    if pd.isna(row[0]) or pd.isna(row[1]) or pd.isna(row[2]):
                        skipped_null_values += 1
                        continue
                        
                    # Parse timestamp
                    timestamp = pd.to_datetime(f"{row[0]} {row[1]}", dayfirst=True)
                    
                    # Validate and clean value
                    try:
                        value = float(row[2])
                        if not np.isfinite(value):
                            skipped_null_values += 1
                            continue
                    except:
                        skipped_null_values += 1
                        continue

                    rows_by_param[current_param].append({
                        "timestamp": timestamp.isoformat(),
                        "parameter_name": current_param,
                        "value": value,
                        "safe_min": safe_min,
                        "safe_max": safe_max
                    })
                except Exception as e:
                    skipped_null_values += 1
                    continue

            print(f"\n🧪 WATER QUALITY PROCESSING SUMMARY")
            print("=" * 60)
            print(f"⚠️ Skipped {skipped_invalid_params} rows with invalid parameter names")
            print(f"⚠️ Skipped {skipped_null_values} rows with invalid/missing values")
            
            total_records = 0
            for param, rows in rows_by_param.items():
                if rows:
                    cutoff = int(len(rows) * INGESTION_PERCENTAGE)
                    historical_data = rows[:cutoff]
                    print(f"📊 {param}: {len(historical_data)} records for ingestion")
                    rows_by_param[param] = historical_data
                    total_records += len(historical_data)
            
            print(f"\n📈 Total valid records prepared: {total_records}")
            return rows_by_param
                
        except Exception as e:
            print(f"❌ Error processing water quality data: {e}")
            return {}
    
    def process_flow_data(self, file_path):
        """Process flow data without calculating flow_rate"""
        try:
            df = pd.read_csv(file_path, header=None)
            print(f"🌊 Flow CSV loaded: {df.shape[0]} rows, {df.shape[1]} columns")
            
            expected_locations = [
                "Corporation Water",
                "Ground Water Source 1", 
                "Ground Water Source 2",
                "Industrial Process",
                "Tanker Water Supply"
            ]
            
            rows_by_location = defaultdict(list)
            current_location = None
            
            for index, row in df.iterrows():
                try:
                    col_a = str(row[0]).strip() if pd.notna(row[0]) else ""
                    col_b = str(row[1]).strip() if pd.notna(row[1]) else ""
                    col_c = str(row[2]).strip() if pd.notna(row[2]) else ""
                    
                    if col_a.startswith("Location Name:"):
                        current_location = col_a.replace("Location Name:", "").strip()
                        continue
                    
                    if col_a.lower() == "date" and col_b.lower() == "time" and col_c.lower() == "totalizer":
                        continue
                    
                    if not col_a and not col_b and not col_c:
                        continue
                    
                    if current_location and col_a and col_b and col_c:
                        try:
                            if col_a != "########":
                                date_str = col_a
                                time_str = col_b
                                totalizer_val = float(col_c)
                                
                                if len(date_str) == 10 and date_str.count('-') == 2:
                                    timestamp = pd.to_datetime(f"{date_str} {time_str}", format="%d-%m-%Y %H:%M:%S")
                                else:
                                    timestamp = pd.to_datetime(f"{date_str} {time_str}")
                                
                                rows_by_location[current_location].append({
                                    "timestamp": timestamp.isoformat(),
                                    "location_name": current_location,
                                    "totalizer": totalizer_val
                                })
                                
                        except Exception as parse_error:
                            continue
                        
                except Exception as row_error:
                    continue

            print(f"\n🌊 FLOW DATA PROCESSING SUMMARY")
            print("=" * 60)
            
            processed_locations = {}
            total_records = 0
            
            for location_name, rows in rows_by_location.items():
                if rows and location_name in expected_locations:
                    rows.sort(key=lambda x: x['timestamp'])
                    
                    cutoff = int(len(rows) * INGESTION_PERCENTAGE)
                    historical_data = rows[:cutoff]
                    
                    print(f"📍 {location_name}: {len(historical_data)} records for ingestion")
                    processed_locations[location_name] = historical_data
                    total_records += len(historical_data)
            
            print(f"\n📈 Total flow records prepared: {total_records}")
            return processed_locations
            
        except Exception as e:
            print(f"❌ Error processing flow rate data: {e}")
            return {}
    
    def insert_data_batch(self, table_name, data, batch_size=1000):
        """Insert data in batches via REST API"""
        try:
            total_inserted = 0
            total_batches = (len(data) + batch_size - 1) // batch_size
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                response = requests.post(
                    f"{self.supabase_url}/rest/v1/{table_name}",
                    headers=self.headers,
                    json=batch,
                    timeout=30
                )
                
                if response.status_code in [200, 201]:
                    print(f"   ✅ Batch {batch_num}/{total_batches}: {len(batch)} records inserted")
                    total_inserted += len(batch)
                else:
                    print(f"   ❌ Batch {batch_num} failed: {response.status_code} - {response.text}")
            
            return total_inserted
            
        except Exception as e:
            print(f"❌ Error inserting batch: {e}")
            return 0
    
    def insert_quality_data(self, quality_data):
        """Insert water quality data via REST API"""
        try:
            total_inserted = 0
            
            for parameter, records in quality_data.items():
                if not records:
                    continue
                    
                print(f"📊 Inserting {parameter} ({len(records)} records)...")
                inserted = self.insert_data_batch('water_quality', records)
                total_inserted += inserted
                
            print(f"🎉 Total quality records inserted: {total_inserted}")
            
        except Exception as e:
            print(f"❌ Error inserting quality data: {e}")
            raise
    
    def insert_flow_data(self, flow_data):
        """Insert water flow data via REST API"""
        try:
            total_inserted = 0
            
            for location, records in flow_data.items():
                if not records:
                    continue
                    
                print(f"🌊 Inserting {location} ({len(records)} records)...")
                inserted = self.insert_data_batch('flow_rate', records)
                total_inserted += inserted
                
            print(f"🎉 Total flow records inserted: {total_inserted}")
            
        except Exception as e:
            print(f"❌ Error inserting flow data: {e}")
            raise
    
    def verify_data(self):
        """Verify inserted data via REST API"""
        try:
            print(f"\n📊 VERIFICATION")
            print("=" * 50)
            
            # Check quality data count
            response = requests.get(
                f"{self.supabase_url}/rest/v1/water_quality?select=count",
                headers=self.headers
            )
            if response.status_code == 200:
                quality_count = response.json()[0]['count']
                print(f"🧪 Water Quality Records: {quality_count}")
            
            # Check flow data count
            response = requests.get(
                f"{self.supabase_url}/rest/v1/flow_rate?select=count",
                headers=self.headers
            )
            if response.status_code == 200:
                flow_count = response.json()[0]['count']
                print(f"🌊 Water Flow Records: {flow_count}")
            
            print("✅ VERIFICATION COMPLETE!")
            
        except Exception as e:
            print(f"❌ Error during verification: {e}")
    
    def run_full_ingestion(self, clear_existing=True):
        """Run the complete ingestion process via REST API"""
        try:
            print("🚀 SUPABASE REST API INGESTION")
            print("=" * 80)
            print(f"🌐 Using REST API instead of direct database connection")
            print(f"📊 Processing {INGESTION_PERCENTAGE*100}% for historical load")
            print("=" * 80)
            
            # Test connection
            if not self.test_connection():
                raise Exception("Failed to connect to Supabase REST API")
            
            if clear_existing:
                self.clear_existing_data()
            
            # Process and insert quality data
            print("\n🧪 STEP 1: PROCESSING WATER QUALITY DATA")
            quality_data = self.process_water_quality_data(QUALITY_FILE)
            
            if quality_data:
                print("\n💾 STEP 2: INSERTING QUALITY DATA")
                # Flatten data for batch insertion
                all_quality_records = []
                for param, records in quality_data.items():
                    all_quality_records.extend(records)
                self.insert_data_batch('water_quality', all_quality_records)
            
            # Process and insert flow data
            print("\n🌊 STEP 3: PROCESSING FLOW DATA")
            flow_data = self.process_flow_data(FLOW_FILE)
            
            if flow_data:
                print("\n💾 STEP 4: INSERTING FLOW DATA")
                # Flatten data for batch insertion
                all_flow_records = []
                for location, records in flow_data.items():
                    all_flow_records.extend(records)
                self.insert_data_batch('flow_rate', all_flow_records)
            
            # Verify
            print("\n🔍 STEP 5: VERIFICATION")
            self.verify_data()
            
        except Exception as e:
            print(f"❌ Ingestion failed: {e}")
            raise

def main():
    """Main function using REST API"""
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        print("❌ Missing environment variables:")
        print("📝 Add to your .env file:")
        print("SUPABASE_URL=https://your-project.supabase.co")
        print("SUPABASE_ANON_KEY=your-anon-key")
        return
    
    if not os.path.exists(QUALITY_FILE):
        print(f"❌ Quality data file not found: {QUALITY_FILE}")
        return
        
    if not os.path.exists(FLOW_FILE):
        print(f"❌ Flow data file not found: {FLOW_FILE}")
        return
    
    print("🌐 Using Supabase REST API")
    
    ingestion = SupabaseRestIngestion(SUPABASE_URL, SUPABASE_ANON_KEY)
    ingestion.run_full_ingestion(clear_existing=True)
    
    print("\n🎉 REST API INGESTION COMPLETE!")

if __name__ == "__main__":
    main()