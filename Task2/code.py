!java -version
!pip install pyspark

# Modeling
from fbprophet import Prophet

# Data 
import pandas as pd

# Spark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, current_date
from sklearn.model_selection import train_test_split

# Local Library
from queries import * 
from model_regressor import *

#Initialising Spakr Session
spark = SparkSession.builder.master("databricks").getOrCreate()
storage_account_name = ''
storage_account_access_key = ''
blob_container = 'mtfiles'
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

# Load data
file_path = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/Weekly_data_for_testing_100.csv"
df = spark.read.format("csv").load(file_path, inferSchema = True, header = True)
print((df.count(), len(df.columns)))

# Select specific items of full list
df_short = df[df['Combine_SKU'].isin(['SKU 4', 'SKU 5', 'SKU 6', 'SKU 7', 'SKU 8', 'SKU 9', 'SKU 10', 'SKU 11'])]
print((df_short.count(), len(df_short.columns)))
df_short = df_short.toPandas()
df_short['date'] =  pd.to_datetime(df_short['date'], infer_datetime_format = True)
dates_df_short = df_short.set_index('date')

# Converting Pandas DataFrame to Spark DataFrame
sdf = spark.createDataFrame(df_short)
sdf.show(5)

# Checking the line Item per SKUs
sdf.select(['Combine_SKU']).groupBy('Combine_SKU').agg({'Combine_SKU':'count'}).show()

# Creating a temporary view so that we can fire sql quiery on it
sdf.createOrReplaceTempView("Sales1")
spark.sql(get_query_item_per_sku()).show()

#Defining ds & y column for Prophet and adding other variables
sql = get_query_derin_ds()
spark.sql(sql).show()
sdf.explain()

#Get the no of Partition
sdf.rdd.getNumPartitions()

# Repartitioning the data based on Combine_SKU column
sku_part = (spark.sql(sql).repartition(spark.sparkContext.defaultParallelism,['Combine_SKU'])).cache()
sku_part.explain()

# Defining Schema
result_schema=StructType([
              StructField('ds', TimestampType()),
              StructField('Combine_SKU', StringType()),
              StructField('y', DoubleType()),
              StructField('yhat', DoubleType()),
              StructField('yhat_upper', DoubleType()),
              StructField('yhat_lower', DoubleType())])

@pandas_udf(result_schema,PandasUDFType.GROUPED_MAP)
def forecast_Sales(store_pd):
    train_data, test_data = train_test_split(store_pd, test_size = 0.2)
    regressors = get_model_regressors()

    model = Prophet(interval_width = 0.95, seasonality_mode = 'multiplicative', weekly_seasonality = True, yearly_seasonality = True)
    
    for regressor in regressors:
      model.add_regressor(regressor)

    model.fit(train_data)
    forecast_pd = model.predict(test_data)
  
    f_pd = forecast_pd[['ds', 'yhat', 'yhat_upper', 'yhat_lower']].set_index('ds')
    st_pd = store_pd[['ds', 'Combine_SKU', 'y']].set_index('ds')
  
    forecast_results_pd = f_pd.join(st_pd, how = 'left')
    forecast_results_pd.reset_index(level = 0, inplace = True)
    forecast_results_pd['Combine_SKU'] = store_pd['Combine_SKU'].iloc[0]
  
    return forecast_results_pd[['ds', 'Combine_SKU', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]
  
forecast_results = (sku_part.groupBy('Combine_SKU').apply(forecast_Sales).withColumn('training_date', current_date()))

forecast_results.cache()
forecast_results.show()
forecast_results.coalesce(1)
forecast_results.count()
forecast_results.createOrReplaceTempView('forecasted')

spark.sql("select Combine_SKU, count(*) from forecasted group by Combine_SKU").show()
final_df = forecast_results.toPandas()