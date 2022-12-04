!java -version
#!pip install pyarrow==0.15.1
!pip install pyspark
from fbprophet import Prophet
from pyspark.sql import SparkSession
import pyspark


#Initialising Spakr Session
spark = SparkSession.builder.master("databricks").getOrCreate()

storage_account_name = ''
storage_account_access_key = ''
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

blob_container = 'mtfiles'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/Weekly_data_for_testing_100.csv"
df = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

print((df.count(), len(df.columns)))


import pandas as pd
#df_short = df
df_short = df[df['Combine_SKU'].isin(['SKU 4','SKU 5','SKU 6','SKU 7','SKU 8','SKU 9','SKU 10','SKU 11'])]
#df_short = df[df['Combine_SKU'].isin(['SKU 5'])]
print((df_short.count(), len(df_short.columns)))



df_short_1 = df_short.toPandas()
df_short_1['date'] =  pd.to_datetime(df_short_1['date'],infer_datetime_format = True)
item_df_short_1 = df_short_1.set_index('date')

# Converting Pandas DataFrame to Spark DataFrame
sdf = spark.createDataFrame(df_short_1)

sdf.show(5)

# Checking the line Item per SKUs
#sdf.select(['Combine_SKU','POS']).groupBy('Combine_SKU','POS').agg({'Combine_SKU':'count'}).show()
sdf.select(['Combine_SKU']).groupBy('Combine_SKU').agg({'Combine_SKU':'count'}).show()

# Creating a temporary view so that we can fire sql quiery on it
sdf.createOrReplaceTempView("Sales1")

spark.sql("select Combine_SKU,POS,POSLag1,POSLag2,POSLag3,POSLag4,Month,Year,Day,WeekHols,WeekSeason,NYE,AUS_DAY,V_Day,Easter,M_DAY,CBRY_CUP,FOOTY_FNL,HLWEEN,XMAS,Price20Percent_OFF,Price30Percent_OFF,Price4Percent_OFF,Price40Percent_OFF,Price50Percent_OFF,Price20Percent_OFFLag1,Price30Percent_OFFLag1,Price4Percent_OFFLag1,Price40Percent_OFFLag1,Price50Percent_OFFLag1,Bench,CAT,INS,BenchLag1,CATLag1,INSLag1,END_FLY_BUYS,END_1,END_2,END_3,END_4,END_6,END_7,END_8,END_9,END_10,END_11,END_12,END_13,END_14,END_15,END_16,END_17,END_18,END_19,END_20,END_21,END_22,END_23,END_24,END_25,END_26,END_27,END_28,END_29,END_30,END_31,END_32,END_33,END_34,END_35,END_36,END_37,END_38,END_39,END_40,END_41,END_42,END_43,END_44,END_45,count(*) from Sales1 group by Combine_SKU, POS,POSLag1,POSLag2,POSLag3,POSLag4,Month,Year,Day,WeekHols,WeekSeason,NYE,AUS_DAY,V_Day,Easter,M_DAY,CBRY_CUP,FOOTY_FNL,HLWEEN,XMAS,Price20Percent_OFF,Price30Percent_OFF,Price4Percent_OFF,Price40Percent_OFF,Price50Percent_OFF,Price20Percent_OFFLag1,Price30Percent_OFFLag1,Price4Percent_OFFLag1,Price40Percent_OFFLag1,Price50Percent_OFFLag1,Bench,CAT,INS,BenchLag1,CATLag1,INSLag1,END_FLY_BUYS,END_1,END_2,END_3,END_4,END_6,END_7,END_8,END_9,END_10,END_11,END_12,END_13,END_14,END_15,END_16,END_17,END_18,END_19,END_20,END_21,END_22,END_23,END_24,END_25,END_26,END_27,END_28,END_29,END_30,END_31,END_32,END_33,END_34,END_35,END_36,END_37,END_38,END_39,END_40,END_41,END_42,END_43,END_44,END_45 order by Combine_SKU").show()

#Defining ds & y column for Prophet and adding other variables
sql = 'SELECT Combine_SKU,POS,POSLag1,POSLag2,POSLag3,POSLag4,Month,Year,Day,WeekHols,WeekSeason,NYE,AUS_DAY,V_Day,Easter,M_DAY,CBRY_CUP,FOOTY_FNL,HLWEEN,XMAS,Price20Percent_OFF,Price30Percent_OFF,Price4Percent_OFF,Price40Percent_OFF,Price50Percent_OFF,Price20Percent_OFFLag1,Price30Percent_OFFLag1,Price4Percent_OFFLag1,Price40Percent_OFFLag1,Price50Percent_OFFLag1,Bench,CAT,INS,BenchLag1,CATLag1,INSLag1,END_FLY_BUYS,END_1,END_2,END_3,END_4,END_6,END_7,END_8,END_9,END_10,END_11,END_12,END_13,END_14,END_15,END_16,END_17,END_18,END_19,END_20,END_21,END_22,END_23,END_24,END_25,END_26,END_27,END_28,END_29,END_30,END_31,END_32,END_33,END_34,END_35,END_36,END_37,END_38,END_39,END_40,END_41,END_42,END_43,END_44,END_45, date as ds,sum(Sales) as y FROM Sales1 GROUP BY Combine_SKU,POS,POSLag1,POSLag2,POSLag3,POSLag4,Month,Year,Day,WeekHols,WeekSeason,NYE,AUS_DAY,V_Day,Easter,M_DAY,CBRY_CUP,FOOTY_FNL,HLWEEN,XMAS,Price20Percent_OFF,Price30Percent_OFF,Price4Percent_OFF,Price40Percent_OFF,Price50Percent_OFF,Price20Percent_OFFLag1,Price30Percent_OFFLag1,Price4Percent_OFFLag1,Price40Percent_OFFLag1,Price50Percent_OFFLag1,Bench,CAT,INS,BenchLag1,CATLag1,INSLag1,END_FLY_BUYS,END_1,END_2,END_3,END_4,END_6,END_7,END_8,END_9,END_10,END_11,END_12,END_13,END_14,END_15,END_16,END_17,END_18,END_19,END_20,END_21,END_22,END_23,END_24,END_25,END_26,END_27,END_28,END_29,END_30,END_31,END_32,END_33,END_34,END_35,END_36,END_37,END_38,END_39,END_40,END_41,END_42,END_43,END_44,END_45,ds ORDER BY Combine_SKU,ds'

spark.sql(sql).show()

sdf.explain()

#Get the no of Partition
sdf.rdd.getNumPartitions()

# Repartitioning the data based on Combine_SKU column
sku_part = (spark.sql(sql).repartition(spark.sparkContext.defaultParallelism,['Combine_SKU'])).cache()

sku_part.explain()

# Defining Schema
from pyspark.sql.types import *
result_schema=StructType([
  StructField('ds',TimestampType()),
   StructField('Combine_SKU',StringType()),
  StructField('y',DoubleType()),
  StructField('yhat',DoubleType()),
  StructField('yhat_upper',DoubleType()),
  StructField('yhat_lower',DoubleType())])

from pyspark.sql.functions import pandas_udf, PandasUDFType
from sklearn.model_selection import train_test_split
@pandas_udf(result_schema,PandasUDFType.GROUPED_MAP)
def forecast_Sales(store_pd):  
    train_1, test_1 = train_test_split(store_pd, test_size=0.2)
  
 
    m=Prophet(interval_width=0.95,seasonality_mode = 'multiplicative', weekly_seasonality = True,yearly_seasonality = True)
  
    m.add_regressor('POS')
    m.add_regressor('POSLag1')
    m.add_regressor('POSLag2')
    m.add_regressor('POSLag3')
    m.add_regressor('POSLag4')
    m.add_regressor('Month')
    m.add_regressor('Year')
    m.add_regressor('Day')
    m.add_regressor('WeekHols')
    m.add_regressor('WeekSeason')
    m.add_regressor('NYE')
    m.add_regressor('AUS_DAY')
    m.add_regressor('V_Day')
    m.add_regressor('Easter')
    m.add_regressor('M_DAY')
    m.add_regressor('CBRY_CUP')
    m.add_regressor('FOOTY_FNL')
    m.add_regressor('HLWEEN')
    m.add_regressor('XMAS')
    m.add_regressor('Price20Percent_OFF')
    m.add_regressor('Price30Percent_OFF')
    m.add_regressor('Price4Percent_OFF')
    m.add_regressor('Price40Percent_OFF')
    m.add_regressor('Price50Percent_OFF')                                                                                
    m.add_regressor('Price20Percent_OFFLag1')
    m.add_regressor('Price30Percent_OFFLag1')
    m.add_regressor('Price4Percent_OFFLag1')
    m.add_regressor('Price4Percent_OFFLag1')
    m.add_regressor('Price40Percent_OFFLag1')
    m.add_regressor('Price50Percent_OFFLag1')
    m.add_regressor('Bench')
    m.add_regressor('CAT')
    m.add_regressor('INS')
    m.add_regressor('BenchLag1')
    m.add_regressor('CATLag1')
    m.add_regressor('INSLag1')
    m.add_regressor('END_FLY_BUYS')
    m.add_regressor('END_1')
    m.add_regressor('END_2')
    m.add_regressor('END_3')
    m.add_regressor('END_4')
    m.add_regressor('END_6')
    m.add_regressor('END_7')
    m.add_regressor('END_8')
    m.add_regressor('END_9')
    m.add_regressor('END_10')
    m.add_regressor('END_11')
    m.add_regressor('END_12')
    m.add_regressor('END_13')
    m.add_regressor('END_14')
    m.add_regressor('END_15')
    m.add_regressor('END_16')
    m.add_regressor('END_17')
    m.add_regressor('END_18')
    m.add_regressor('END_19')
    m.add_regressor('END_20')
    m.add_regressor('END_21')
    m.add_regressor('END_22')
    m.add_regressor('END_23')
    m.add_regressor('END_24')
    m.add_regressor('END_25')
    m.add_regressor('END_26')
    m.add_regressor('END_27')
    m.add_regressor('END_28')
    m.add_regressor('END_29')
    m.add_regressor('END_30')
    m.add_regressor('END_31')
    m.add_regressor('END_32')
    m.add_regressor('END_33')
    m.add_regressor('END_34')
    m.add_regressor('END_35')
    m.add_regressor('END_36')
    m.add_regressor('END_37')
    m.add_regressor('END_38')
    m.add_regressor('END_39')
    m.add_regressor('END_40')
    m.add_regressor('END_41')
    m.add_regressor('END_42')
    m.add_regressor('END_43')
    m.add_regressor('END_44')
    m.add_regressor('END_45') 
  
    m.fit(train_1)
  
    # future_pd=model.make_future_dataframe(test_1)
  
    forecast_pd= m.predict(test_1)
  
    f_pd= forecast_pd[['ds','yhat','yhat_upper','yhat_lower']].set_index('ds')
    st_pd=store_pd[['ds','Combine_SKU','y']].set_index('ds')
  
    results_pd=f_pd.join(st_pd,how='left')
    results_pd.reset_index(level=0,inplace=True)
    results_pd['Combine_SKU']=store_pd['Combine_SKU'].iloc[0]
  
    return results_pd[['ds','Combine_SKU','y','yhat','yhat_upper','yhat_lower']]
  

from pyspark.sql.functions import current_date
results=(sku_part.groupBy('Combine_SKU').apply(forecast_Sales).withColumn('training_date',current_date()))

results.cache()

results.show()

results.coalesce(1)

results.count()

results.createOrReplaceTempView('forecasted')

spark.sql("select Combine_SKU,count(*) from forecasted group by Combine_SKU").show()

final_df = results.toPandas()


