import json
from datetime import datetime
from pyspark.sql import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pymongo

# create a SparkSession
spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

def dfToParquet(x_dataframe:DataFrame,x_def_name,x_comment):
    ## Salvar os dados processados em formato Parquet.
    ## Garantir que os arquivos sejam particionados por originState e destinationState.
    x_dataframe.write.partitionBy("originState", "destinationState").parquet("/output/proto.parquet")
    return
