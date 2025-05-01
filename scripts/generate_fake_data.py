from faker import Faker
import pandas as pd
from random import seed, choice, uniform, randint
import uuid
from datetime import timedelta
from pathlib import Path

fake = Faker()
Faker.seed(42)
seed(42)

# Pointing out the path to save the data
OUTPUT_DIR = Path("../data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Create the directory if it doesn't already exist. 

NUM_CUSTOMERS = 5000
NUM_PROPERTIES = 3000
NUM_TRANSACTIONS = 4500
NUM_CSATS = 1500
MINIMUM_HOUSE_PRICE = 150000
MAXIMUM_HOUSE_PRICE = 750000

# 1. Customers
customers = []  # Initialising an empty list to capture the customer's details

for customer in range(NUM_CUSTOMERS):
    customers.append(
        {
            'customer_id': str(uuid.uuid4())[:8],
            'full_name': fake.name(),
            'email': fake.email(),
            'signup_date': fake.date_between(start_date='-3y', end_date='-1d'),
            'region': choice(["North West", "London", "South East", "Scotland", "East Midlands", "West Midlands", "South West", "Yorkshire and the Humber"]),
            'source_channel': choice(["Website", "Referral", "Estate Agent"])
        }
    )  # Pulling mock data to fit the customer schema

df_customers = pd.DataFrame(customers)
df_customers.to_csv(OUTPUT_DIR / "customers.csv", index=False)

# 2. Properties = []

properties = []
for _ in range(NUM_PROPERTIES):
    properties.append(
        {
            'property_id': str(uuid.uuid4())[:5],
            'address': fake.street_address(),
            'postcode': fake.postcode(),
            'property_type': choice(["Flat", "Detached", "Terraced", "Semi-detached"]),
            'valuation': round(uniform(MINIMUM_HOUSE_PRICE, MAXIMUM_HOUSE_PRICE), 2),
            'region': choice(["North West", "London", "South East", "Scotland", "East Midlands", "West Midlands", "South West", "Yorkshire and the Humber"])
        }
    )

df_properties = pd.DataFrame(properties)
df_properties.to_csv(OUTPUT_DIR / "properties.csv", index=False)


# 3. Transactions

transactions = []
transactions_ids = []

for transaction in range(NUM_TRANSACTIONS):
    customer = choice(customers)
    property_choice = choice(properties)
    start_date = fake.date_between(start_date='-2y', end_date='-20d')
    status = choice(["Started", "Under Offer", "Completed", "Abandoned"])
    complete_date = fake.date_between(start_date=start_date, end_date='today') if status == "Completed" else "N/A"
    transactions_id = str(uuid.uuid4())
    transactions_ids.append(transactions_id)

    transactions.append(
        {
            'transaction_id': transactions_id,
            'customer_id': customer['customer_id'],
            'property_id': property_choice['property_id'],
            'solicitor_name': fake.name(),
            'status': status,
            'start_date': start_date,
            'completion_date': complete_date, 
            'current_stage': choice(['Search', 'Contracts', 'Survey', 'Offer', 'N/A']) if status != 'Completed' else "N/A"
        }
    )

df_transactions = pd.DataFrame(transactions)
df_transactions.to_csv(OUTPUT_DIR / "transactions.csv", index=False)


# 4. CSAT (Customer Satisfaction)

csat = []
for review in range(NUM_CSATS):
    transaction = choice(transactions)
    if transaction['status'] != 'Abadoned':
        csat.append(
            {
                'survey_id': str(uuid.uuid4())[:6],
                'transaction_id': transaction['transaction_id'],
                'survey_date': fake.date_between(start_date=transaction['start_date'], end_date='today'),
                'score': randint(1, 5),
                'comment': fake.sentence(nb_words=15)
            }
        )

df_csat = pd.DataFrame(csat)
df_csat.to_csv(OUTPUT_DIR / "csat_surveys.csv", index=False)