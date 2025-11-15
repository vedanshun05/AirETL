"""
End-to-end pipeline test
"""
import time
import subprocess
import sys

def test_pipeline():
    print("🧪 Testing Complete Pipeline\n")
    
    # Test 1: Send sample1.json
    print("📤 Test 1: Sending sample1.json...")
    result = subprocess.run(
        [sys.executable, "producer.py", "../data/sample1.json"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    
    time.sleep(2)
    
    # Test 2: Send sample2.json
    print("\n📤 Test 2: Sending sample2.json...")
    result = subprocess.run(
        [sys.executable, "producer.py", "../data/sample2.json"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    
    time.sleep(2)
    
    # Test 3: Send sample3.json
    print("\n📤 Test 3: Sending sample3.json...")
    result = subprocess.run(
        [sys.executable, "producer.py", "../data/sample3.json"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    
    print("\n✅ All test data sent!")
    print("👀 Check the consumer terminal to see the results")

if __name__ == "__main__":
    test_pipeline()