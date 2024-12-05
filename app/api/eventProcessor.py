import json
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from . import utilitiesDataframe


# create a SparkSession
spark = SparkSession.builder.appName("ReadJSON").getOrCreate()


def dfJsonLoad(p:str, x_path:str='data/input_data.json'):
    print('*  Running... dfJsonLoad()')
    ## 1.1) Ler o JSON em um DataFrame PySpark.
    # Load JSON FILE into DataFrame 
    if x_path == '': x_path = "/Users/rudaruda/Documents/Repos/fastapi-pipeline/data/input_data.json"
    df = spark.read.json(x_path, multiLine=True)
    utilitiesDataframe.dfToMongo(df,'dfJsonLoad',f'carregou arquivo de: {x_path}')
    print('*  Finish! dfJsonLoad()')
    return utilitiesDataframe.dfOutput(p,df)


def dfJsonSearchItemList(p:str,x_dataframe:DataFrame=None):
    print('*  Running... dfJsonSearchItemList()')
    ## 1.2) Explodir o array searchItemsList para normalizar os dados.
    # Read attribute "searchItemsList" into JSON FILE
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    if "data" in df.columns:
        df = json.loads(df.select("data.searchItemsList").toJSON().collect()[0])['searchItemsList']
        df = spark.createDataFrame(data=df, schema = ["name","properties"])
        utilitiesDataframe.dfToMongo(df,'dfJsonSearchItemList',f'limpeza do dado, seleção do atributo "searchItemsList"')
    print('* Finish! dfJsonSearchItemList()')
    return utilitiesDataframe.dfOutput(p,df)


def dfJsonFeatureCols(p:str,x_dataframe:DataFrame=None):
    print('*  Running... dfJsonFeatureCols()')
    ## 1.3) Criar colunas derivadas: 'departure_datetime', 'arrival_datetime' e 'route'
    # ADD new columns: departure_datetime, arrival_datetime, route
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    df = utilitiesDataframe.dfContactTwoCols("departure_datetime", "departureDate", "departureHour", df)
    df = utilitiesDataframe.dfContactTwoCols("arrival_datetime", "arrivalDate", "arrivalHour", df)
    df = utilitiesDataframe.dfContactTwoCols("route", "originCity", "destinationCity", df)
    print('*  Finish! dfJsonFeatureCols()')
    return utilitiesDataframe.dfOutput(p,df)


def dfFilterDeparturesFutures(p:str,x_dataframe:DataFrame=None):
    print('*  Running... dfFilterDeparturesFutures()')
    ## 1.4) Viagens futuras (baseadas no departure_datetime).
    # Em testes não foi preciso realizar a conversão do dado para timestamp: # df = df.withColumn("departure_datetime", F.col("departure_datetime").cast("timestamp"))
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    df = df.filter(F.col("departure_datetime") > F.current_timestamp())
    print('*  Finish! dfFilterDeparturesFutures()')
    return utilitiesDataframe.dfOutput(p,df)


def dfFilterSeatsAvailables(p:str,x_dataframe:DataFrame=None):
    print('*  Running... dfFilterSeatsAvailables()')
    ## 1.5) Viagens com availableSeats > 0
    # Em testes não foi preciso realizar a conversão do dado para integer: # df = df.withColumn("departure_datetime", F.col("departure_datetime").cast("integer"))
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    df = df.filter(F.col("availableSeats") > 0)
    print('*  Finish! dfFilterSeatsAvailables()')
    return utilitiesDataframe.dfOutput(p,df)


def process_events():
    print('** Running... process_events()')
    ## EventProcessor: ■ Leia o JSON. ■ Normalize os dados. ■ Retorne o DataFrame processado
    # x Leia o JSON
    df = dfJsonLoad('')
    # x Normalize os dados.
    df = dfJsonSearchItemList('df',df)
    df = dfJsonFeatureCols('df',df)
    df = dfFilterDeparturesFutures('df',df)
    df = dfFilterSeatsAvailables('df',df)
    #■ Retorne o DataFrame processado
    utilitiesDataframe.dfToMongo(df,'process_events()','carregou JSON, normalizou e retornou DataFrame')
    print('** Finish! process_events()')
    return df