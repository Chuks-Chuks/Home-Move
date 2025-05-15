from constant_class import Constants
# from pathlib import Path

const = Constants()

"""
This table will help measure the customer satisfaction scores by property_type
"""
spark = const.start_spark('csat_prop_to_gold')
customer_feedback = spark.read.parquet(f'{const.SILVER_PATH}/customer_feedback')
customer_feedback.limit(5).show()

trx_sum = spark.read.parquet(f'{const.SILVER_PATH}/property_transactions')

# Merging both tables to see which property the customer purchased and gave a review
merged_df = customer_feedback.join(trx_sum, on='customer_id', how='left') \
.groupBy('property_type') \
.agg(
    const.psq.count('*').alias('num_responses'),
    const.psq.avg('score').alias('average_scores'),
    sum(const.psq.when(const.psq.col('score') == 5, 1).otherwise(0)).alias('perfect_score'),
) \
.withColumn('percent_perfect', (const.psq.col('perfect_score') / const.psq.col('num_responses')) * 100)


merged_df.limit(5).show()

merged_df.write.mode('overwrite').parquet(f'{const.GOLD_PATH}/csat_prop_kpi')