import pandas as pd
from collections import defaultdict
import io
from datetime import datetime, timedelta
import numpy as np

QUALITY_FILE = "data/water_quality_data.csv"
FLOW_FILE = "data/water_flow_data.csv"

# -------- WATER QUALITY DRY RUN --------
def process_water_quality_dryrun(file_path):
    try:
        df = pd.read_csv(file_path, header=None)
        print(f"📊 Water Quality CSV loaded: {len(df)} rows")
        
        rows_by_param = defaultdict(list)
        current_param = None
        safe_min, safe_max = None, None

        for _, row in df.iterrows():
            line = str(row[0])

            if "Safe Range" in line:
                param_name = line.split("Safe Range:")[0].split(" ", 1)[1].strip()
                param_name = param_name.replace("(HUMIDITY)", "HUMIDITY").strip()
                safe_range_str = line.split("Safe Range:")[1].strip(" ()")
                safe_min, safe_max = [float(x) for x in safe_range_str.split("to")]
                current_param = param_name
                continue

            if "Date" in line or "Time" in str(row[1]) or pd.isna(row[0]):
                continue

            try:
                timestamp = pd.to_datetime(f"{row[0]} {row[1]}", dayfirst=True)
                value = float(row[2])
                rows_by_param[current_param].append({
                    "timestamp": timestamp,
                    "parameter_name": current_param,
                    "value": value,
                    "safe_min": safe_min,
                    "safe_max": safe_max
                })
            except Exception as e:
                continue

        print("\n🧪 WATER QUALITY PARAMETERS ANALYSIS")
        print("=" * 60)
        
        for param, rows in rows_by_param.items():
            if rows:
                values = [r["value"] for r in rows]
                cutoff = int(len(rows) * 0.8)
                
                print(f"\n📊 {param}")
                print(f"   Safe Range: {rows[0]['safe_min']} to {rows[0]['safe_max']}")
                print(f"   Total Records: {len(rows)}")
                print(f"   Will Process: {cutoff} (80%)")
                print(f"   Min Value: {min(values):.2f}")
                print(f"   Max Value: {max(values):.2f}")
                print(f"   Average: {np.mean(values):.2f}")
                print(f"   Out of Range Count: {len([v for v in values if v < rows[0]['safe_min'] or v > rows[0]['safe_max']])}")
                
        return rows_by_param
            
    except Exception as e:
        print(f"❌ Error processing water quality data: {e}")
        return {}

# -------- COMPLETELY REWRITTEN FLOW RATE PROCESSING --------
def process_flow_rate_dryrun(file_path):
    try:
        # Read the CSV file without assuming headers
        df = pd.read_csv(file_path, header=None)
        print(f"\n🌊 Flow CSV loaded: {df.shape[0]} rows, {df.shape[1]} columns")
        
        print("First 10 rows of raw data:")
        print(df.head(10))
        
        # Expected locations
        expected_locations = [
            "Corporation Water",
            "Ground Water Source 1", 
            "Ground Water Source 2",
            "Industrial Process",
            "Tanker Water Supply"
        ]
        
        # Parse the data structure based on your Excel format
        rows_by_location = defaultdict(list)
        total_processed = 0
        total_skipped = 0
        current_location = None
        
        print(f"\n🔍 PARSING CSV STRUCTURE...")
        
        for index, row in df.iterrows():
            try:
                # Convert row to string values for easier parsing
                col_a = str(row[0]).strip() if pd.notna(row[0]) else ""
                col_b = str(row[1]).strip() if pd.notna(row[1]) else ""
                col_c = str(row[2]).strip() if pd.notna(row[2]) else ""
                
                # Debug print for first 20 rows
                if index < 20:
                    print(f"Row {index}: A='{col_a}' | B='{col_b}' | C='{col_c}'")
                
                # Check if this row contains a location header
                if col_a.startswith("Location Name:"):
                    current_location = col_a.replace("Location Name:", "").strip()
                    print(f"📍 Found location section: {current_location}")
                    continue
                
                # Check if this is a header row (Date, Time, Totalizer)
                if col_a.lower() == "date" and col_b.lower() == "time" and col_c.lower() == "totalizer":
                    print(f"📋 Found header row for location: {current_location}")
                    continue
                
                # Skip empty rows
                if not col_a and not col_b and not col_c:
                    continue
                
                # Try to parse as data row if we have a current location
                if current_location and col_a and col_b and col_c:
                    try:
                        # Parse date and time
                        if col_a != "########":  # Skip malformed date entries
                            # Try different date formats
                            date_str = col_a
                            time_str = col_b
                            totalizer_val = float(col_c)
                            
                            # Create timestamp
                            try:
                                # Try DD-MM-YYYY format first
                                if len(date_str) == 10 and date_str.count('-') == 2:
                                    timestamp = pd.to_datetime(f"{date_str} {time_str}", format="%d-%m-%Y %H:%M:%S")
                                else:
                                    timestamp = pd.to_datetime(f"{date_str} {time_str}")
                            except:
                                # Fallback to automatic parsing
                                timestamp = pd.to_datetime(f"{date_str} {time_str}")
                            
                            rows_by_location[current_location].append({
                                "timestamp": timestamp,
                                "location_name": current_location,
                                "totalizer": totalizer_val
                            })
                            total_processed += 1
                            
                    except Exception as parse_error:
                        total_skipped += 1
                        if index < 50:  # Only show parse errors for first 50 rows
                            print(f"   Parse error row {index}: {parse_error}")
                        continue
                else:
                    total_skipped += 1
                    continue
                    
            except Exception as row_error:
                total_skipped += 1
                continue

        print(f"\n📊 PROCESSING SUMMARY")
        print(f"✅ Successfully processed: {total_processed} rows")
        print(f"❌ Skipped: {total_skipped} rows")
        
        print(f"\n🏭 LOCATION-WISE FLOW ANALYSIS")
        print("=" * 80)
        
        # Detailed analysis for each location
        for location_name, rows in sorted(rows_by_location.items()):
            if rows:
                totalizer_values = [r["totalizer"] for r in rows]
                cutoff = int(len(rows) * 0.8)
                
                # Calculate flow rates (difference between consecutive totalizer readings)
                flow_rates = []
                for i in range(1, len(rows)):
                    time_diff = (rows[i]["timestamp"] - rows[i-1]["timestamp"]).total_seconds() / 3600  # hours
                    totalizer_diff = rows[i]["totalizer"] - rows[i-1]["totalizer"]
                    if time_diff > 0 and totalizer_diff >= 0:
                        flow_rate = totalizer_diff / time_diff  # per hour
                        flow_rates.append(flow_rate)
                
                print(f"\n📍 {location_name}")
                print(f"   📊 Total Records: {len(rows)}")
                print(f"   🎯 Will Process: {cutoff} (80% of total)")
                print(f"   🔢 Totalizer Range: {min(totalizer_values):.3f} - {max(totalizer_values):.3f}")
                print(f"   📈 Average Totalizer: {np.mean(totalizer_values):.3f}")
                
                if flow_rates:
                    print(f"   🌊 Flow Rate Analysis:")
                    print(f"      Min Flow Rate: {min(flow_rates):.3f} units/hour")
                    print(f"      Max Flow Rate: {max(flow_rates):.3f} units/hour") 
                    print(f"      Avg Flow Rate: {np.mean(flow_rates):.3f} units/hour")
                    print(f"      Flow Variations: {len([f for f in flow_rates if f > np.mean(flow_rates) * 1.5])} peak periods")
                
                # Time pattern analysis
                if len(rows) > 1:
                    time_span = rows[-1]["timestamp"] - rows[0]["timestamp"]
                    print(f"   ⏰ Time Span: {time_span.days} days, {time_span.seconds//3600} hours")
                    
                    # Sample timestamps to show data range
                    print(f"   📅 First Record: {rows[0]['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"   📅 Last Record: {rows[-1]['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                
                print(f"   ✅ Status: {'✓ Active' if max(totalizer_values) > min(totalizer_values) else '⚠ Inactive/No Flow'}")
        
        # Summary statistics
        print(f"\n📋 OVERALL SUMMARY")
        print("=" * 50)
        print(f"🏭 Total Locations Found: {len(rows_by_location)}")
        print(f"📍 Locations List:")
        for i, location in enumerate(sorted(rows_by_location.keys()), 1):
            record_count = len(rows_by_location[location])
            totalizer_values = [r["totalizer"] for r in rows_by_location[location]]
            status = "Active" if (totalizer_values and max(totalizer_values) > min(totalizer_values)) else "Inactive"
            print(f"   {i}. {location} ({record_count} records) - {status}")
        
        # Check for expected locations
        print(f"\n🔍 EXPECTED LOCATIONS CHECK:")
        found_locations = set(rows_by_location.keys())
        
        for expected_loc in expected_locations:
            if expected_loc in found_locations:
                print(f"   ✅ {expected_loc} - Found")
            else:
                print(f"   ❌ {expected_loc} - Missing")
                # Check for partial matches
                partial_matches = [loc for loc in found_locations 
                                 if expected_loc.lower() in loc.lower() or loc.lower() in expected_loc.lower()]
                if partial_matches:
                    print(f"      🔍 Possible matches: {', '.join(partial_matches)}")
        
        # Show all found locations for debugging
        print(f"\n📍 ALL FOUND LOCATIONS ({len(found_locations)}):")
        for i, loc in enumerate(sorted(found_locations), 1):
            count = len(rows_by_location[loc])
            print(f"   {i:2d}. '{loc}' ({count} records)")
        
        return rows_by_location
        
    except Exception as e:
        print(f"❌ Error processing flow rate data: {e}")
        import traceback
        traceback.print_exc()
        return {}

# -------- ENHANCED DATA PATTERN ANALYSIS --------
def analyze_data_patterns(quality_data, flow_data):
    """Analyze patterns across both datasets"""
    
    print(f"\n🔄 CROSS-DATASET PATTERN ANALYSIS")
    print("=" * 60)
    
    if quality_data:
        total_quality_records = sum(len(records) for records in quality_data.values())
        print(f"📊 Total Quality Records: {total_quality_records}")
        print(f"📋 Quality Parameters: {len(quality_data)}")
    
    if flow_data:
        total_flow_records = sum(len(records) for records in flow_data.values()) 
        print(f"🌊 Total Flow Records: {total_flow_records}")
        print(f"📍 Flow Locations: {len(flow_data)}")
        
        # Calculate data density (records per day)
        if total_flow_records > 0 and len(flow_data) > 0:
            # More realistic estimation based on actual data
            avg_records_per_location = total_flow_records / len(flow_data)
            estimated_days = avg_records_per_location / 96  # Assuming 15-min intervals
            print(f"📅 Average Records per Location: {avg_records_per_location:.0f}")
            print(f"📅 Estimated Days of Data: {estimated_days:.1f}")
            print(f"⏱️  Expected 15-min Intervals: {estimated_days * 96:.0f} per location")
    
    print(f"\n💡 RECOMMENDATIONS FOR REAL-TIME SIMULATION:")
    print("- Use 1-5 second intervals for demo (speed up 900x-4500x)")
    print("- Focus on locations with highest variation for interesting demos")
    print("- Set quality alerts based on safe range violations")
    print("- Highlight peak usage patterns in analytics")

# -------- MAIN EXECUTION --------
if __name__ == "__main__":
    print("🚀 WATER MANAGEMENT DATA ANALYSIS - STRUCTURE-AWARE VERSION")
    print("=" * 80)
    print("🎯 Analyzing 5-Location Flow Data + Quality Parameters")
    print("🔧 FIXED: CSV structure parsing for Excel export format")
    print("=" * 80)
    
    # Process quality data
    print("\n🧪 STEP 1: PROCESSING WATER QUALITY DATA")
    quality_results = process_water_quality_dryrun(QUALITY_FILE)
    
    # Process flow data  
    print("\n🌊 STEP 2: PROCESSING FLOW RATE DATA (5 LOCATIONS)")
    flow_results = process_flow_rate_dryrun(FLOW_FILE)
    
    # Cross-analysis
    print("\n🔍 STEP 3: PATTERN ANALYSIS")
    analyze_data_patterns(quality_results, flow_results)
    
    print(f"\n🎉 ANALYSIS COMPLETE!")
    print("=" * 50)
    print("✅ Ready for real-time simulation integration")
    print("🚀 Next step: Update React component with this data structure")