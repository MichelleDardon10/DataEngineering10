import requests
import random
from datetime import datetime, timedelta, timezone
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "http://localhost:8082/api/v1/trips"

# Station IDs (NYC has ~1000 stations)
STATIONS = list(range(1, 100))

# Bike types
BIKE_TYPES = ["electric", "classic", "docked"]

def generate_trip(trip_num: int) -> dict:
    """Generate a realistic bike trip"""
    start_time = datetime.now(timezone.utc) - timedelta(hours=random.randint(0, 24))
    duration = random.randint(180, 3600)  # 3 min to 1 hour
    end_time = start_time + timedelta(seconds=duration)
    
    # Occasionally generate bad data to test quality scoring
    if random.random() < 0.05:  # 5% bad data
        # Bad data scenarios
        scenario = random.choice([
            "duration_mismatch",
            "invalid_time",
            "excessive_duration",
            "suspicious_age",
            "same_station"
        ])
        
        if scenario == "duration_mismatch":
            duration = duration + 3600  # Wrong duration
        elif scenario == "invalid_time":
            end_time = start_time - timedelta(seconds=100)  # End before start
        elif scenario == "excessive_duration":
            duration = 90000  # 25 hours
        elif scenario == "suspicious_age":
            return {
                "trip_id": f"trip_{trip_num}_{int(time.time())}",
                "bike_id": random.randint(1, 1000),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "start_station_id": random.choice(STATIONS),
                "end_station_id": random.choice(STATIONS),
                "rider_age": random.choice([5, 150]),  # Invalid age
                "trip_duration": duration,
                "bike_type": random.choice(BIKE_TYPES)
            }
        elif scenario == "same_station":
            station = random.choice(STATIONS)
            return {
                "trip_id": f"trip_{trip_num}_{int(time.time())}",
                "bike_id": random.randint(1, 1000),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "start_station_id": station,
                "end_station_id": station,  # Same station
                "rider_age": random.randint(16, 75),
                "trip_duration": 120,  # Short trip
                "bike_type": random.choice(BIKE_TYPES)
            }
    
    return {
        "trip_id": f"trip_{trip_num}_{int(time.time())}",
        "bike_id": random.randint(1, 1000),
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "start_station_id": random.choice(STATIONS),
        "end_station_id": random.choice(STATIONS),
        "rider_age": random.randint(16, 75),
        "trip_duration": duration,
        "bike_type": random.choice(BIKE_TYPES)
    }

def send_trip(trip: dict) -> tuple[bool, float, str]:
    """Send a single trip to the API"""
    start = time.time()
    try:
        response = requests.post(API_URL, json=trip, timeout=5)
        elapsed = time.time() - start
        
        if response.status_code == 200:
            return True, elapsed, trip["trip_id"]
        else:
            return False, elapsed, f"Error {response.status_code}: {response.text}"
    except Exception as e:
        elapsed = time.time() - start
        return False, elapsed, str(e)

def test_single_trip():
    """Test sending a single trip"""
    print("=== Single Trip Test ===")
    trip = generate_trip(1)
    print(f"\nSending trip: {json.dumps(trip, indent=2)}")
    
    success, elapsed, result = send_trip(trip)
    
    if success:
        print(f"✅ Success! Response time: {elapsed*1000:.2f}ms")
        print(f"Trip ID: {result}")
    else:
        print(f"❌ Failed: {result}")
        print(f"Response time: {elapsed*1000:.2f}ms")

def load_test(num_trips: int = 100, concurrency: int = 10):
    """Load test with multiple concurrent requests"""
    print(f"\n=== Load Test: {num_trips} trips, {concurrency} concurrent ===")
    
    trips = [generate_trip(i) for i in range(num_trips)]
    results = {"success": 0, "failed": 0, "times": []}
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_trip, trip) for trip in trips]
        
        for future in as_completed(futures):
            success, elapsed, result = future.result()
            results["times"].append(elapsed)
            
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1
                print(f"❌ Failed: {result}")
    
    total_time = time.time() - start_time
    
    # Calculate statistics
    times = results["times"]
    avg_time = sum(times) / len(times) if times else 0
    min_time = min(times) if times else 0
    max_time = max(times) if times else 0
    throughput = num_trips / total_time
    
    print(f"\n📊 Results:")
    print(f"  Total trips: {num_trips}")
    print(f"  Successful: {results['success']} ({results['success']/num_trips*100:.1f}%)")
    print(f"  Failed: {results['failed']} ({results['failed']/num_trips*100:.1f}%)")
    print(f"  Total time: {total_time:.2f}s")
    print(f"  Throughput: {throughput:.2f} trips/sec")
    print(f"\n⏱️  Response Times:")
    print(f"  Average: {avg_time*1000:.2f}ms")
    print(f"  Min: {min_time*1000:.2f}ms")
    print(f"  Max: {max_time*1000:.2f}ms")

def stress_test(duration_seconds: int = 60, trips_per_second: int = 100):
    """Stress test: sustained load for a duration"""
    print(f"\n=== Stress Test: {trips_per_second} trips/sec for {duration_seconds}s ===")
    
    start_time = time.time()
    trip_num = 0
    results = {"success": 0, "failed": 0}
    
    with ThreadPoolExecutor(max_workers=50) as executor:
        while time.time() - start_time < duration_seconds:
            batch_start = time.time()
            
            # Submit batch
            futures = []
            for _ in range(trips_per_second):
                trip = generate_trip(trip_num)
                future = executor.submit(send_trip, trip)
                futures.append(future)
                trip_num += 1
            
            # Wait for batch to complete
            for future in as_completed(futures):
                success, _, _ = future.result()
                if success:
                    results["success"] += 1
                else:
                    results["failed"] += 1
            
            # Sleep to maintain rate
            elapsed = time.time() - batch_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            
            # Progress update every 10 seconds
            if int(time.time() - start_time) % 10 == 0:
                current_throughput = trip_num / (time.time() - start_time)
                print(f"  {int(time.time() - start_time)}s: {trip_num} trips sent ({current_throughput:.1f}/sec)")
    
    total_time = time.time() - start_time
    actual_throughput = trip_num / total_time
    
    print(f"\n📊 Stress Test Results:")
    print(f"  Total trips sent: {trip_num}")
    print(f"  Successful: {results['success']} ({results['success']/trip_num*100:.1f}%)")
    print(f"  Failed: {results['failed']} ({results['failed']/trip_num*100:.1f}%)")
    print(f"  Duration: {total_time:.2f}s")
    print(f"  Throughput: {actual_throughput:.2f} trips/sec")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python test_client.py single              # Test single trip")
        print("  python test_client.py load [trips] [concurrency]  # Load test")
        print("  python test_client.py stress [duration] [rate]    # Stress test")
        sys.exit(1)
    
    test_type = sys.argv[1]
    
    if test_type == "single":
        test_single_trip()
    elif test_type == "load":
        trips = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        concurrency = int(sys.argv[3]) if len(sys.argv) > 3 else 10
        load_test(trips, concurrency)
    elif test_type == "stress":
        duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
        rate = int(sys.argv[3]) if len(sys.argv) > 3 else 100
        stress_test(duration, rate)
    else:
        print(f"Unknown test type: {test_type}")