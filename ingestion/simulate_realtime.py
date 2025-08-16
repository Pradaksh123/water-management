import time
import random
from datetime import datetime
from supabase import create_client
import os
from dotenv import load_dotenv
import threading

load_dotenv()

class RealtimeDataSimulator:
    def __init__(self):
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_ANON_KEY")
        )
        self.parameters = [
            "HUMIDITY", "ETP (TDS)", "ETP (pH)",
            "STP (TDS)", "STP (TSS)", "STP (BOD)",
            "STP (pH)", "STP (COD)"
        ]
        self.locations = [
            "Corporation Water",
            "Ground Water Source 1",
            "Ground Water Source 2",
            "Industrial Process",
            "Tanker Water Supply"
        ]
        self.safe_ranges = {
            "HUMIDITY": (30, 70),
            "ETP (TDS)": (100, 1000),
            "ETP (pH)": (6.5, 9.0),
            "STP (TDS)": (100, 1000),
            "STP (TSS)": (1000, 3000),
            "STP (BOD)": (0, 5),
            "STP (pH)": (6.5, 9.0),
            "STP (COD)": (1000, 3000)
        }
        self.running = False

    def generate_realistic_value(self, param, location):
        """Generate values with location-specific patterns"""
        base_value = random.uniform(*self.safe_ranges[param])
        
        # Location-specific adjustments
        if "Ground Water" in location:
            base_value *= 0.9  # Typically lower values
        elif "Industrial" in location:
            base_value *= 1.1  # Typically higher values
            
        # 5% chance of out-of-range for demo
        if random.random() < 0.05:
            return base_value * random.choice([0.8, 1.2])
        return base_value

    def simulate_readings(self):
        """Generate one set of readings across all parameters/locations"""
        timestamp = datetime.now().isoformat()
        quality_data = []
        flow_data = []
        
        for location in self.locations:
            # Quality data
            for param in self.parameters:
                quality_data.append({
                    "timestamp": timestamp,
                    "parameter_name": param,
                    "value": round(self.generate_realistic_value(param, location), 2),

                    "safe_min": self.safe_ranges[param][0],
                    "safe_max": self.safe_ranges[param][1]
                })
            
            # Flow data
            flow_data.append({
                "timestamp": timestamp,
                "location_name": location,
                "totalizer": round(random.uniform(1000, 5000), 2)
            })
        
        return quality_data, flow_data

    def insert_data(self, data, table_name):
        """Insert data with error handling"""
        try:
            response = self.supabase.table(table_name).insert(data).execute()
            if response.data:
                print(f"✅ Real-time: Added {len(data)} records to {table_name}")
                return True
        except Exception as e:
            print(f"❌ Real-time insert failed: {e}")
        return False

    def run_simulation(self, interval=30):
        """Run continuous simulation"""
        self.running = True
        print("🚀 Starting real-time simulation...")
        
        while self.running:
            quality_data, flow_data = self.simulate_readings()
            
            # Insert data
            self.insert_data(quality_data, "water_quality")
            self.insert_data(flow_data, "flow_rate")
            
            time.sleep(interval)

    def start(self):
        """Start in background thread"""
        thread = threading.Thread(target=self.run_simulation, daemon=True)
        thread.start()
        return thread

def start_realtime():
    simulator = RealtimeDataSimulator()
    try:
        simulator.run_simulation(interval=20)  # Faster updates for demo
    except KeyboardInterrupt:
        simulator.running = False
        print("Simulation stopped")

if __name__ == "__main__":
    start_realtime()