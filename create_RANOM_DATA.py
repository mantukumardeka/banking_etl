import pandas as pd
import random
from faker import Faker

fake = Faker()

# ----------------- Create Customers Table -----------------
customers = []
for i in range(1, 31):
    customers.append({
        "customer_id": i,
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "city": fake.city(),
        "state": fake.state(),
        "country": fake.country(),
        "created_at": fake.date_between(start_date='-2y', end_date='today')
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv("/Users/mantukumardeka/Desktop/DataEngineering/banking_etl.customers.csv", index=False)



# ----------------- Create Transactions Table -----------------
transaction_types = ["Deposit", "Withdrawal"]
statuses = ["Completed", "Pending"]

transactions = []
for i in range(1, 31):
    transactions.append({
        "transaction_id": i,
        "customer_id": random.randint(1, 30),
        "amount": round(random.uniform(100, 1000), 2),
        "transaction_type": random.choice(transaction_types),
        "transaction_date": fake.date_between(start_date='-1y', end_date='today'),
        "status": random.choice(statuses),
        "branch": fake.city() + " Branch"
    })

transactions_df = pd.DataFrame(transactions)
transactions_df.to_csv("/Users/mantukumardeka/Desktop/DataEngineering/banking_etl.transactions.csv", index=False)

print("Generated 'customers.csv' and 'transactions.csv' with 30 rows each.")
