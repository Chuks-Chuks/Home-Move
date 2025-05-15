from faker import Faker
import pandas as pd
from random import seed, choice, uniform, randint
import uuid
from pathlib import Path
from scripts.constant_class import Constants
import logging

# Setup logging
class IngestData(Constants):
    def __init__(self):
        super().__init__()
        self.fake = Faker()
        Faker.seed(42)
        seed(42)
        self.customers = []
        self.properties = []
        # Pointing out the path to save the data
        self.OUTPUT_DIR = Path(self.RAW_PATH)
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  
        self.transactions = []
        self.transactions_ids = []
        self.csat = []
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

# Create the directory if it doesn't already exist. 
     # 1. Customers
    def fetch_customers(self) -> pd.DataFrame:
        """
        This method ingests the customer's data

        :returns:
            Pandas dataframe of the customer's details
        """
        try:
            for customer in range(self.NUM_CUSTOMERS):
                self.customers.append(
                    {
                        'customer_id': str(uuid.uuid4())[:8],
                        'full_name': self.fake.name(),
                        'email': self.fake.email(),
                        'signup_date': self.fake.date_between(start_date='-3y', end_date='-1d'),
                        'region': choice(["North West", "London", "South East", "Scotland", "East Midlands", "West Midlands", "South West", "Yorkshire and the Humber"]),
                        'source_channel': choice(["Website", "Referral", "Estate Agent"])
                    }
                )  # Pulling mock data to fit the customer schema

            df_customers = pd.DataFrame(self.customers)
            self.logger.info("The customers' details have been saved successfully")
            df_customers.to_csv(self.OUTPUT_DIR / "customers.csv", index=False)
            return df_customers
        except Exception as e:
            self.logger.error(f"An error has occurred and the customers' upload has failed:{str(e)}")
            raise

    # 2. Properties = []
    def fetch_properties(self) -> None:
        """
        Fetches all property details.

        :returns:
            None (Writes csv file to raw folder)
        """
        try:
            for _ in range(self.NUM_PROPERTIES):
                self.properties.append(
                    {
                        'property_id': str(uuid.uuid4())[:5],
                        'address': self.fake.street_address(),
                        'postcode': self.fake.postcode(),
                        'property_type': choice(["Flat", "Detached", "Terraced", "Semi-detached"]),
                        'valuation': round(uniform(self.MINIMUM_HOUSE_PRICE, self.MAXIMUM_HOUSE_PRICE), 2),
                        'region': choice(["North West", "London", "South East", "Scotland", "East Midlands", "West Midlands", "South West", "Yorkshire and the Humber"])
                    }
                )

            df_properties = pd.DataFrame(self.properties)
            self.logger.info("Properties fetched successfuy")
            df_properties.to_csv(self.OUTPUT_DIR / "properties.csv", index=False)
            return df_properties
        except Exception as e:
            self.logger.error(f"Failed to fetch properties: {str(e)}")
            raise

    # 3. Transactions
    def fetch_transactions(self) -> None:
        """
        Fetches all the transactions 

        :returns:   
            None (Writes transaction details to the raw folder)
            
        """
        if not self.customers or not self.properties:
            raise ValueError("Customers or properties data not loaded")
        try:
            self.logger.info(f"Starting transactions with {len(self.customers)} customers and {len(self.properties)} properties")
            for transaction in range(self.NUM_TRANSACTIONS):
                customer = choice(self.customers)
                property_choice = choice(self.properties)
                start_date = self.fake.date_between(start_date='-2y', end_date='-20d')
                status = choice(["Started", "Under Offer", "Completed", "Abandoned"])
                complete_date = self.fake.date_between(start_date=start_date, end_date='today') if status == "Completed" else "N/A"
                transactions_id = str(uuid.uuid4())
                self.transactions_ids.append(transactions_id)

                self.transactions.append(
                    {
                        'transaction_id': transactions_id,
                        'customer_id': customer['customer_id'],
                        'property_id': property_choice['property_id'],
                        'solicitor_name': self.fake.name(),
                        'status': status,
                        'start_date': start_date,
                        'completion_date': complete_date, 
                        'current_stage': choice(['Search', 'Contracts', 'Survey', 'Offer', 'N/A']) if status != 'Completed' else "N/A"
                    }
                )

            df_transactions = pd.DataFrame(self.transactions)
            self.logger.info("Transactions fetched successfuy")
            df_transactions.to_csv(self.OUTPUT_DIR / "transactions.csv", index=False)
            return df_transactions
        except Exception as e:
            self.logger.error(f"Failed to fetch transactions {str(e)}")
            raise



    # 4. CSAT (Customer Satisfaction)
    def fetch_cust_feedback(self) -> None:
        """
            Fetches all customer feedback.

            :returns:
                None (Writes csv file to raw folder)
        """
        try:
            if self.transactions and isinstance(self.transactions[0]['start_date'], str):
                for t in self.transactions:
                    t['start_date'] = pd.to_datetime(t['start_date']).date()

            for review in range(self.NUM_CSATS):
                transaction = choice(self.transactions)
                if transaction['status'] != 'Abandoned':
                    self.csat.append(
                        {
                            'survey_id': str(uuid.uuid4())[:6],
                            'transaction_id': transaction['transaction_id'],
                            'survey_date': self.fake.date_between(start_date=transaction['start_date'], end_date='today'),
                            'score': randint(1, 5),
                            'comment': self.fake.sentence(nb_words=15)
                        }
                    )

            df_csat = pd.DataFrame(self.csat)
            self.logger.info("Customer feedback fetched successfully")
            df_csat.to_csv(self.OUTPUT_DIR / "csat.csv", index=False)
            return df_csat
        except Exception as e:
            self.logger.error(f"The customer feedback data has failed to load: {str(e)}")
            raise