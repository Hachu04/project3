import random
import string
import csv

NUM_CUSTOMERS = 50000
NUM_PURCHASES = 5000000

def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_address():
    # Simple random address generator
    return random_string(random.randint(15, 30)) + " St, " + random_string(random.randint(5, 10))

def generate_customers(filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for cust_id in range(1, NUM_CUSTOMERS + 1):
            name = random_string(random.randint(10, 20))
            age = random.randint(18, 100)
            address = random_address()
            salary = round(random.uniform(1000, 10000), 2)
            writer.writerow([cust_id, name, age, address, salary])

def generate_purchases(filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for trans_id in range(1, NUM_PURCHASES + 1):
            cust_id = random.randint(1, NUM_CUSTOMERS)
            trans_total = round(random.uniform(10, 2000), 2)
            num_items = random.randint(1, 15)
            desc = random_string(random.randint(20, 50))
            writer.writerow([trans_id, cust_id, trans_total, num_items, desc])

if __name__ == "__main__":
    print("Generating Customers.csv ...")
    generate_customers("Customers.csv")
    print("Generating Purchases.csv ...")
    generate_purchases("Purchases.csv")
    print("Done.")