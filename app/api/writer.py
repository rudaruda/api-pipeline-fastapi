import json
from datetime import datetime
from pyspark.sql import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pymongo

# create a SparkSession
spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

def dfToMongo(x_df:DataFrame,x_def_name,x_comment):
    mdb_cli = pymongo.MongoClient("mongodb://root:toor@localhost:27017/")
    mdb_db = mdb_cli["project"]
    mdb_col = mdb_db["dataframes"]
    row_ins = { "dataframe":str(x_df.collect()), "schema":str(x_df.schema), "dthr":datetime.now()}
    if type(x_def_name) is str: row_ins['function'] = x_def_name
    if type(x_comment) is str: row_ins['comment'] = x_comment
    mdb_col.insert_one(row_ins)
    return

def dfGetMongo()->DataFrame:
    mdb_cli = pymongo.MongoClient("mongodb://root:toor@localhost:27017/")
    mdb_db = mdb_cli["project"]
    mdb_col = mdb_db["dataframes"]
    x_df = mdb_col.find( 
        {"dataframe": {"$exists": True, "$ne": None}, "schema": {"$exists": True, "$ne": None}}
        ,{"dataframe": 1, "schema": 1, "_id": 0}
        ).sort("dthr", -1).limit(1)
    x_data, x_schema= x_df[0]['dataframe'], x_df[0]['schema']
    return spark.createDataFrame(data=eval(x_data),schema=eval(x_schema))

def dfOutput(x_type:str, x_dataframe:DataFrame):
    res = ''
    if x_type == 'json': res = toJson(x_dataframe.toJSON().collect()) 
    elif x_type == 'df': res = x_dataframe
    else: res = x_dataframe.schema.json()
    return res

def toJson(x_json):
    try:
        res = json.loads(x_json) 
    except:
        res = json.loads(json.dumps(x_json))
    return x_json

def dfContactTwoCols(new_col:str, x_col1:str, x_col2:str, x_dataframe:DataFrame)->DataFrame:
    x_cols = x_dataframe.columns
    if x_col1 in x_cols and x_col2 in x_cols and not new_col in x_cols: 
        x_dataframe = x_dataframe.withColumn( new_col, F.concat( F.col(x_col1), F.lit(" "), F.col(x_col2)))
        dfToMongo(x_dataframe,'dfContactTwoCols','concatenou colunas')#f'concatenou colunas: {new_col}={x_col1}+{x_col2}'
        print(f'* concatenou: {new_col}={x_col1}+{x_col2}')
    return x_dataframe

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
    

dfJsonLoad("data/input_data.json")

#def df_json_filter_departure_datetime():
    ## 1.4) Viagens futuras (baseadas no departure_datetime).


#
#"departure_datetime": F.when( F.col("departureDate").isNotNull() & F.col("departureHour").isNotNull(),  str(F.col("departureDate")) + ' ' + str(F.col("departureHour")) ) 
#df = df.withColumns({
#        "departure_datetime": str(F.col("departureDate")) + " " + str(F.col("departureHour"))
#        , "arrival_datetime": str(F.col("arrivalDate")) + " " + str(F.col("arrivalHour"))
#        , "route": str(F.col("originCity")) + " " + str(F.col("destinationCity"))
#    })
#    df_searchItemsList = df
#    df_searchItemsList.cache()
#    return df_searchItemsList.schema.json()


""".
■ Filtrar dados:
■ Viagens futuras (baseadas no departure_datetime).
■ Viagens com availableSeats > 0.
"""
