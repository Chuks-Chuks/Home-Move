from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff, count, sum, avg
from pathlib import Path

# Start spark

spark = SparkSession.builder.appName('Transform Bronze to Silver').getOrCreate()

# Define Paths 
BRONZE_PATH = './data/bronze'
SILVER_PATH = './data/silver'

Path(SILVER_PATH).mkdir(parents=True, exist_ok=True)  # Creates the directory if it doesn't exists. 

# Read the parquet files
customers = spark.read.parquet(f'{BRONZE_PATH}/customers')
# print(f'{customers}\n')
properties = spark.read.parquet(f'{BRONZE_PATH}/properties')
# print(f'{properties}\n')
transactions = spark.read.parquet(f'{BRONZE_PATH}/transactions')
# print(f'{transactions}\n')
csat = spark.read.parquet(f'{BRONZE_PATH}/csat')
# print(f'{csat}\n')

"""
    Finding out the customer data, properties and what stage they are in now. 
    Transaction_file has both customer and property IDs. Both tables would be joined to the transactions table
"""

customer_transactions = transactions \
    .join(customers, 'customer_id', 'left') \
    .join(properties, 'property_id') \
        .select(
    'transaction_id',
    'customer_id',
    'full_name',
    'email',
    'property_id',
    'address',
    'valuation',
    'status',
    'start_date',
    'completion_date'
)

# print(customer_transactions.printSchema())  # The results showed the completion_date to be a string.

# Also, i will be dropping values with null to ensure the dataset is compact and complete 
# Converting dates to date type

customer_transactions = customer_transactions.withColumn('start_date', to_date('start_date')) \
.withColumn('completion_date', to_date('completion_date'))

# print(customer_transactions.printSchema()) # The date format is now correct

# Write the transformed customer_transactions to the Silver folder ()
customer_transactions.write.mode('overwrite').parquet(f'{SILVER_PATH}/customer_transactions')

"""
    The aim of the join is to measure customer satisfaction.
    The customer table and customer feedback table will be joined. This is see the likelihood of customers recommending Movera
"""

feedback_df = csat.join(transactions, on='transaction_id', how='left') \
.join(customers, on='customer_id', how='left') \
.select(
    'customer_id',
    'full_name',
    'email',
    'score',
    col('comment').alias('feedback'),
    'survey_date'
)

feedback_df.write.mode('overwrite').parquet(f'{SILVER_PATH}/customer_feedback')

"""
    The next join solves the question of how many houses have been purchased by how much, which customer
"""

property_transactions = transactions.join(properties, on='property_id', how='left') \
.join(customers, on='customer_id', how='left') \
.select(
    'transaction_id',
    'customer_id',
    'full_name',
    'email',
    'property_id',
    'address', 
    'property_type',
    'valuation',
    'status',
    'start_date',
    'completion_date'
)

property_transactions.write.mode('overwrite').parquet(f'{SILVER_PATH}/property_transactions')

"""
    The next join is to find out the valuable customers. 
    I will be finding out:
        - The number of homes bought or sold
        - Total amount and average spent
        - Check how many feedbacks has been given, average feedback
"""

# First, aggregating the transactions by customer_id

transaction_summary = transactions.join(properties, on='property_id', how='left').groupBy('customer_id').agg(
    count('*').alias('num_transactions'),  # Finding the total number of transactions
    sum('valuation').alias('total_spent'),  # Finding the total amount spent
    avg('valuation').alias('avg_spent')  # Finding the average amount spent
)

feedback_summary = csat.join(transactions, on='transaction_id', how='left') \
.join(customers, on='customer_id', how='left').groupBy(
    'customer_id'
).agg(
    count('*').alias('num_feedbacks'),
    avg('score').alias('avg_feedback_score')
)

# Merging both transaction and summary tables

summary_df = customers.join(feedback_summary, on='customer_id', how='left') \
.join(transaction_summary, on='customer_id', how='left') \
.fillna(
    {
        'num_transactions': 0,
        'total_spent': 0,
        'avg_spent': 0.0,
        'num_feedbacks': 0,
        'avg_feedback_score': 0.0
    }
)

summary_df.write.mode('overwrite').parquet(f'{SILVER_PATH}/customer_summary')

"""
    This is to check how much houses are worth in various locations
"""

property_summary = properties.groupBy('region', 'property_type') \
.agg(
    count('*').alias('num_properties'),
    avg('valuation').alias('avg_valuation'),
    min('valuation').alias('min_valuation'),
    max('valuation').alias('max_valuation')
)

property_summary.write.mode('overwrite').parquet(f'{SILVER_PATH}/property_summary')


"""
    I will creating a transaction summary to detail which properties are being bought the most, total and average price. 
"""

transaction_summary = transactions.join(properties, on='property_id', how='left') \
.groupBy('property_type', 'status') \
.agg(
    count('*').alias('num_transactions'),
    sum('valuation').alias('total_value'),
    avg('valuation').alias('avg_valuation')
)
# transaction_summary.show()

transaction_summary.write.mode('overwrite').parquet(f'{SILVER_PATH}/transaction_summary')