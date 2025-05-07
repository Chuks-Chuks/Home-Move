from constant_class import Constants

const = Constants()


"""
Computing the revenue summary monthly by property_type
"""
spark = const.start_spark(title_of_app='Montly revenue for Properties sold')
transaction_summary = spark.read.parquet(f'{const.SILVER_PATH}/transaction_summary')

revenue_summary_monthly_prop = transaction_summary.filter(const.psq.col('status') == 'Completed') \
.groupBy('property_type', 'start_month') \
.agg(
    const.psq.count('*').alias('num_sales'),
    const.psq.sum('valuation').alias('total_revenue'),
    const.psq.avg('valuation').alias('avg_revenue')
) \
.orderBy('start_month')

# revenue_summary_monthly_prop.limit(2).show()

# Writing to the gold table
revenue_summary_monthly_prop.write.mode('overwrite').parquet(f'{const.GOLD_PATH}/revenue_summary_monthly_prop')