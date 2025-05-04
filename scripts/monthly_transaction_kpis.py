from constant_class import Constants
from pathlib import Path

const = Constants()
SILVER_PATH = const.SILVER_PATH
GOLD_PATH = const.GOLD_PATH

Path(GOLD_PATH).mkdir(parents=True, exist_ok=True)
# Starting a Spark Session
spark = const.start_spark('MonthlyGoldLayer')

"""
This transformation here will answer the question how the business is performing month over month in terms of completed property 
transactions and revenue
"""

transactions_sum = spark.read.parquet(f'{SILVER_PATH}/transaction_summary')


completed_summary = transactions_sum.filter(const.psq.col('status') == 'Completed')

# Grouping to show KPIs

monthly_kpis = completed_summary.groupBy('property_type','region','start_month') \
.agg(
    const.psq.count('*').alias('num_transactions'),
    const.psq.sum('valuation').alias('total_value'),
    const.psq.max('valuation').alias('max_value'),
    const.psq.min('valuation').alias('min_value'),
    const.psq.avg('valuation').alias('avg_value')
)

#  monthly_kpis.show()

monthly_kpis.write.mode('overwrite').parquet(f'{GOLD_PATH}/monthly_completed_kpis')
