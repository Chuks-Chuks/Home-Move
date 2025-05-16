from pathlib import Path
import logging

try:
    from scripts.constant_class import Constants
except ImportError as ie:
    logging.warning("Import failed! Now trying alternative solution")
    import sys
    sys.path.insert(0, '/opt/airflow')
    from scripts.constant_class import Constants

const = Constants()

# Start spark
spark = const.start_spark('Transform Bronze to Silver')


# Define Paths 
BRONZE_PATH = const.BRONZE_PATH
SILVER_PATH = const.SILVER_PATH

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

customer_transactions = transactions.alias('trnxs') \
    .join(customers.alias('cust'), on='customer_id', how='left') \
    .join(properties.alias('prop'), on='property_id', how='left') \
    .select(
        'trnxs.transaction_id',
        'cust.customer_id',
        'cust.full_name',
        'cust.email',
        'prop.property_id',
        'prop.address',
        'prop.valuation',
        'trnxs.status',
        'trnxs.start_date',
        'trnxs.completion_date'
)

# print(customer_transactions.printSchema())  # The results showed the completion_date to be a string.

# Also, i will be dropping values with null to ensure the dataset is compact and complete 
# Converting dates to date type

customer_transactions = customer_transactions.withColumn('start_date', const.psq.to_date('start_date')) \
.withColumn('completion_date', const.psq.to_date('completion_date'))

# print(customer_transactions.printSchema()) # The date format is now correct

# Write the transformed customer_transactions to the Silver folder ()
customer_transactions.write.mode('overwrite').parquet(f'{SILVER_PATH}/customer_transactions')

"""
    The aim of the join is to measure customer satisfaction. Check for solicitors who are linked with customer's who have performed.
    The customer table and customer feedback table will be joined. This is see the likelihood of customers recommending Movera
"""

feedback_df = csat.alias('csat').join(transactions.alias('trx'), on='transaction_id', how='left') \
.join(customers.alias('cust'), on='customer_id', how='left') \
.select(
    'cust.customer_id',
    'trx.transaction_id',
    'cust.full_name',
    'cust.email',
    'cust.region',
    'csat.score',
    'trx.solicitor_name',
    const.psq.col('csat.comment').alias('feedback'),
    'csat.survey_date'
)

feedback_df.write.mode('overwrite').parquet(f'{SILVER_PATH}/customer_feedback')

"""
    The next join solves the question of how many houses have been purchased by how much, which customer
"""

property_transactions = transactions.alias('trx').join(properties.alias('prop'), on='property_id', how='left') \
.join(customers.alias('cust'), on='customer_id', how='left') \
.select(
    'trx.transaction_id',
    'cust.customer_id',
    'cust.full_name',
    'cust.email',
    'prop.property_id',
    'prop.address', 
    'prop.property_type',
    'prop.valuation',
    'trx.status',
    'trx.start_date',
    'trx.completion_date',
    'trx.solicitor_name'
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

transaction_summary = transactions.alias('trx').join(properties.alias('prop'), on='property_id', how='left').groupBy('trx.customer_id').agg(
    const.psq.count('trx.*').alias('num_transactions'),  # Finding the total number of transactions
    const.psq.sum('prop.valuation').alias('total_spent'),  # Finding the total amount spent
   const.psq.avg('prop.valuation').alias('avg_spent')  # Finding the average amount spent
)

feedback_summary = csat.alias('csat').join(transactions.alias('trx'), on='transaction_id', how='left') \
.join(customers.alias('cust'), on='customer_id', how='left').groupBy(
    'cust.customer_id'
).agg(
    const.psq.count('csat.*').alias('num_feedbacks'),
    const.psq.avg('csat.score').alias('avg_feedback_score')
)

# Merging both transaction and summary tables

summary_df = customers.alias('cust').join(feedback_summary.alias('fbs'), on='customer_id', how='left') \
.join(transaction_summary.alias('ts'), on='customer_id', how='left') \
.fillna(
    {
        'ts.num_transactions': 0,
        'ts.total_spent': 0,
        'ts.avg_spent': 0.0,
        'fbs.num_feedbacks': 0,
        'fbs.avg_feedback_score': 0.0
    }
)

summary_df.write.mode('overwrite').parquet(f'{SILVER_PATH}/customer_summary')

"""
    This is to check how much houses are worth in various locations
"""

property_summary = properties.groupBy('region', 'property_type') \
.agg(
    const.psq.count('*').alias('num_properties'),
    const.psq.avg('valuation').alias('avg_valuation'),
    const.psq.min('valuation').alias('min_valuation'),
    const.psq.max('valuation').alias('max_valuation')
)

property_summary.write.mode('overwrite').parquet(f'{SILVER_PATH}/property_summary')


"""
    I will creating a transaction summary to detail which properties are being bought the most, total and average price. 
"""

transaction_summary = transactions.alias('trx').join(properties.alias('prop'), on='property_id', how='left') \
.groupBy('prop.property_type', 'trx.status') \
.agg(
    const.psq.count('trx.*').alias('num_transactions'),
    const.psq.sum('prop.valuation').alias('total_value'),
    const.psq.avg('prop.valuation').alias('avg_valuation')
)
# transaction_summary.show()

transaction_summary.write.mode('overwrite').parquet(f'{SILVER_PATH}/transaction_summary')
