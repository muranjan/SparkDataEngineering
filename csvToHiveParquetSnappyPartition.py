from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql.functions import udf, col, input_file_name

from pyspark.sql.types import *

def get_load_date(filepath):
  # This file path name will split on "/" and last part having file name will be splitted again on underscore "_"
  loaddate = filepath.split('/')[-1].split('_')
  return loaddate[1]+'/'+loaddate[2]+'/'+loaddate[3]

#As Column type is not callable, so we need to register it with udf.
load_date_udf =  udf ( get_load_date, StringType() )

sc = SparkContext(appName="CSV_Hive_Partition")
sqlContext = SQLContext(sc)

def load_data( filename ):
  df = sqlContext.read.format("com.databricks.spark.csv")\
  .option("delimiter","|")\
  .option("header", "false")\
  .option("mode","DROPMAFORMED")\
  .schema(schema)\
  .load(filename)
  
  #Add extra column 'load_date' which will have date from filename for hive partition. 
  #Input_file_name() will pass every files passed as the * in load_date
  df = df.withColumn('load_date', load_date_udf(input_file_name()))
  return df

schema = StructType([
  structField("column_1", StringType(), True),
  structField("column_2", StringType(), True),
  structField("column_3", StringType(), True),
  structField("column_4", StringType(), True),
  structField("column_5", StringType(), True),
  structField("column_6", StringType(), True),
  structField("column_7", StringType(), True),
  structField("column_8", StringType(), True)
])

df = load_data('/user/YOUR/HDFS/PATH/sourcefeed_2020_06_01_12_file.csv')

print(df.head(5))

#Below code will create a folder as load_date=2020/06/01 in the path given below. which will be used as Hive partitions.
df.write.mode('overwrite').partitionBy('load_date').parquet('/user/HIVE/WAREHOUSE/YOUR/HIVE/TABLE/PATH/')
  
  
