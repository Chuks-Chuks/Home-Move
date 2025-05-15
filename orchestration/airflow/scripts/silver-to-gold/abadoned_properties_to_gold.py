from constant_class import Constants

const = Constants()

"""
Finding those properties that were never closed. 
"""
spark = const.start_spark(title_of_app='Abandoned Propeties Summary On Monthy Basis')
properties = spark.read.parquet(f'{const.BRONZE_PATH}/properties')

property_abandoned = spark.read.parquet(f'{const.SILVER_PATH}/customer_transactions').filter(const.psq.col('status') == 'Abandoned') \
.withColumn('month', const.psq.date_format('start_date', 'yyyy-MM')) \
.groupby('property_id', 'month') \
.agg(
    const.psq.count('*').alias('num_properties'),
    const.psq.avg('valuation').alias('avg_valuation')
) \
.join(properties, on='property_id', how='left') \
.select(
    'property_type',
    'property_id',
    'month',
    'num_properties',
    'avg_valuation',
    
) \
.orderBy('month')
# property_abandoned.limit(5).show()

property_abandoned.write.mode('overwrite').parquet(f'{const.GOLD_PATH}/abandoned_properties')