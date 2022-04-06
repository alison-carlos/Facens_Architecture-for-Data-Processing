
# Importando bibliotecas

import findspark
findspark.init()
import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

import pandas as pd
import pyspark.pandas as ps

from pyspark.sql import functions as f
from pyspark.sql import types as t
from datetime import datetime

#  Criando sessão do spark + configurações de conexão com bucket 

spark = SparkSession.builder.appName('Steam API - Tratamento').getOrCreate()

def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.path.style.access', 'true')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://localhost:9000')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.connection.ssl.enabled', 'false')
    
load_config(spark.sparkContext)


#  Lendo dados do bucket 

df = spark.read.json('s3a://kafka-bucket/topics/steam/*', multiLine=True)
df.count()

df.withColumn('last_played', f.date_format(df.last_played.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))  \
  .withColumn('timestamp_created', f.date_format(df.timestamp_created.cast(dataType=t.TimestampType()), "yyyy-MM-dd")) \
  .withColumn('timestamp_updated', f.date_format(df.timestamp_updated.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))

df2 = df.withColumn('last_played', f.to_date(df.last_played.cast(dataType=t.TimestampType()))) \
        .withColumn('timestamp_created', f.to_date(df.timestamp_created.cast(dataType=t.TimestampType()))) \
        .withColumn('timestamp_updated', f.to_date(df.timestamp_updated.cast(dataType=t.TimestampType())))

df2 = df2.withColumn("last_played",f.to_timestamp(df2['last_played'])) \
         .withColumn("timestamp_created",f.to_timestamp(df2['timestamp_created'])) \
         .withColumn("timestamp_updated",f.to_timestamp(df2['timestamp_updated']))
         
df2.write.parquet('/home/acsantos/Downloads/teste.parquet')