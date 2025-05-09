from constant_class import Constants
from pathlib import Path

const = Constants()

Path(const.REPORTING_PATH).mkdir(parents=True, exist_ok=True)

spark = const.start_spark('Power BI Reporting')

gold_monthly_kpi = spark.read.parquet(f'{const.GOLD_PATH}/monthly_completed_kpis')
# gold_monthly_kpi.limit(5).show()

# Creating the parquet file for Power BI

# 1. Monthly-completed KPIs
renamed_dictionary = {
    'property_type': 'Property Type',
    'region': 'Region',
    'start_month': 'Start Month',
    'num_transactions': 'Number of Transactions',
    'total_value': 'Total Value',
    'max_value': 'Maximum Value',
    'min_value': 'Mininum Value',
    'avg_value': 'Average Value'
}
report_monthly_completed_kpi = gold_monthly_kpi.withColumnsRenamed(renamed_dictionary)
report_monthly_completed_kpi.write.mode('overwrite').parquet(f'{const.REPORTING_PATH}/report_monthly_completed_kpi')

# 2. Customer Satisfaction and Properties

gold_csat_prop = spark.read.parquet(f'{const.GOLD_PATH}/csat_prop_kpi')
# gold_csat_prop.limit(5).show()
csat_enamed_dictionary = {
    'property_type': 'Property Type',
    'num_responses': 'Number of Responses',
    'average_scores': 'Average Score',
    'perfect_score': 'Perfect score',
    'percent_perfect': 'Percentage of Perfect Score'
}
report_csat_prop_kpi = gold_csat_prop.withColumnsRenamed(csat_enamed_dictionary)
report_csat_prop_kpi.write.mode('overwrite').parquet(f'{const.REPORTING_PATH}/report_csat_prop_kpi')

# Lastly, for abadoned properties

gold_abandoned_properties = spark.read.parquet(f'{const.GOLD_PATH}/abandoned_properties')
gold_abandoned_properties.limit(5).show()

ap_renamed_dictionary = {
    'property_type': 'Property Type',
    'property_id': 'Property ID',
    'month': 'Month',
    'num_properties': 'Number of Properties',
    'avg_valuation': 'Average Valuation'
}

report_abandoned_property_kpi = gold_abandoned_properties.withColumnsRenamed(ap_renamed_dictionary)
report_abandoned_property_kpi.write.mode('overwrite').parquet(f'{const.REPORTING_PATH}/report_abandoned_property_kpi')