import json
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from .utilities import *


# create a SparkSession
spark = SparkSession.builder.appName("ReadJSON").getOrCreate()


def dfJsonLoad(x_path:str)->DataFrame:
    ## 1.1) Ler o JSON em um DataFrame PySpark.
    # Load JSON FILE into DataFrame 
    if x_path == '': x_path = "/Users/rudaruda/Documents/Repos/fastapi-pipeline/data/input_data.json"
    df = spark.read.json(x_path, multiLine=True)
    dfToMongo(df,'dfJsonLoad',f'carregou arquivo de: {x_path}')
    return df


def dfJsonSchema(p:str, x_dataframe:DataFrame=None):
    # Read Schema of JSON FILE
    df = type(x_dataframe) is DataFrame and x_dataframe or dfGetMongo()
    return dfOutput(p,df)


def dfJsonSearchItemList(p:str,x_dataframe:DataFrame=None):
    ## 1.2) Explodir o array searchItemsList para normalizar os dados.
    # Read attribute "searchItemsList" into JSON FILE
    df = type(x_dataframe) is DataFrame and x_dataframe or dfGetMongo()
    if "data" in df.columns:
        df = json.loads(df.select("data.searchItemsList").toJSON().collect()[0])['searchItemsList']
        df = spark.createDataFrame(data=df, schema = ["name","properties"])
        dfToMongo(df,'dfJsonSearchItemList',f'limpeza do dado, seleção do atributo "searchItemsList"')
    return dfOutput(p,df)


def dfJsonFeatureCols(p:str,x_dataframe:DataFrame=None):
    ## 1.3) Criar colunas derivadas: 'departure_datetime', 'arrival_datetime' e 'route'
    # ADD new columns: departure_datetime, arrival_datetime, route
    df = type(x_dataframe) is DataFrame and x_dataframe or dfGetMongo()
    df = dfContactTwoCols("departure_datetime", "departureDate", "departureHour", df)
    df = dfContactTwoCols("arrival_datetime", "arrivalDate", "arrivalHour", df)
    df = dfContactTwoCols("route", "originCity", "destinationCity", df)
    return dfOutput(p,df)


def dfFilterDeparturesFutures(p:str,x_dataframe:DataFrame=None):
    ## 1.4) Viagens futuras (baseadas no departure_datetime).
    # Em testes não foi preciso realizar a conversão do dado para timestamp: # df = df.withColumn("departure_datetime", F.col("departure_datetime").cast("timestamp"))
    df = type(x_dataframe) is DataFrame and x_dataframe or dfGetMongo()
    df = df.filter(F.col("departure_datetime") < F.current_timestamp())
    return dfOutput(p,df)


def dfFilterSeatsAvailables(p:str,x_dataframe:DataFrame=None):
    ## 1.5) Viagens com availableSeats > 0
    # Em testes não foi preciso realizar a conversão do dado para integer: # df = df.withColumn("departure_datetime", F.col("departure_datetime").cast("integer"))
    df = type(x_dataframe) is DataFrame and x_dataframe or dfGetMongo()
    df = df.filter(F.col("availableSeats") > 0)
    return dfOutput(p,df)


def dfContactTwoCols(new_col:str, x_col1:str, x_col2:str, x_dataframe:DataFrame)->DataFrame:
    x_cols = x_dataframe.columns
    if x_col1 in x_cols and x_col2 in x_cols and not new_col in x_cols: 
        x_dataframe = x_dataframe.withColumn( new_col, F.concat( F.col(x_col1), F.lit(" "), F.col(x_col2)))
        dfToMongo(x_dataframe,'dfContactTwoCols','concatenou colunas')#f'concatenou colunas: {new_col}={x_col1}+{x_col2}'
        print(f'* concatenou: {new_col}={x_col1}+{x_col2}')
    return x_dataframe


# Start to LOAD FILE
dfJsonLoad("data/input_data.json")
