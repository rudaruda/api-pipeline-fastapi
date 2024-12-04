import json
from datetime import datetime
from pyspark.sql import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pymongo


# create a SparkSession
spark = SparkSession.builder.appName("ReadJSON").getOrCreate()


def dfGetPriceAvgRouteClasse(p:str,x_dataframe:DataFrame=None):
    ## 2.1) Calcular o preço médio por rota e classe de serviço. 
    df = (x_dataframe
        .groupBy("route","serviceClass")
        .agg(F.avg("travelPrice	").alias("AVG Price"))
        .orderBy("route","serviceClass")
        .show())
    return df   


#def dfFilterSeatsAvailables(p:str,x_dataframe:DataFrame=None):
#    ## 1.5) Viagens com availableSeats > 0
#    # Em testes não foi preciso realizar a conversão do dado para integer: # df = df.withColumn("departure_datetime", F.col("departure_datetime").cast("integer"))
#    df = type(x_dataframe) is DataFrame and x_dataframe or dfGetMongo()
#    df = df.filter(F.col("availableSeats") > 0)
#    return dfOutput(p,df)
