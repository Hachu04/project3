import csv
import random

# Configuration
NUM_RECORDS = 2000000
NUM_TABLES = 100000
SICK_PROBABILITY = 0.05

def generate_data():
    attendees = []
    
    # 1. Generate core data in memory
    for i in range(1, NUM_RECORDS + 1):
        p_id = f"p{i}"
        p_name = f"Person_{i}"
        p_table = f"T{random.randint(1, NUM_TABLES)}"
        # Determine health status 
        is_sick = random.random() < SICK_PROBABILITY
        p_test = "sick" if is_sick else "not-sick"
        
        attendees.append({
            "id": p_id,
            "name": p_name,
            "table": p_table,
            "test": p_test
        })

    # 2. Create Meta-Event.csv (Full Data) 
    with open('Meta-Event.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "table", "test"])
        writer.writerows(attendees)

    # 3. Create Meta-Event-No-Disclosure.csv (No Health Status)
    with open('Meta-Event-No-Disclosure.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "table"])
        for person in attendees:
            writer.writerow({"id": person["id"], "name": person["name"], "table": person["table"]})

    # 4. Create Reported-Illnesses.csv (Only sick people)
    with open('Reported-Illnesses.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "test"])
        for person in attendees:
            if person["test"] == "sick":
                writer.writerow({"id": person["id"], "test": "sick"})

    print("Data generation complete: 3 files created.")

if __name__ == "__main__":
    generate_data()