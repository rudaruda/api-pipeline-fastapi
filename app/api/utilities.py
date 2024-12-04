import json, re
from datetime import datetime
from pyspark.sql import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pymongo


def dfToMongo(x_df:DataFrame,x_def_name,x_comment):
    # Persiste DataFrame num Banco de dados MongoDB
    # Para rastreabilidade e consumo do ultimo DataFrame gerado no processo de ETL com FastAPI
    mdb_cli = pymongo.MongoClient("mongodb://root:toor@localhost:27017/")
    mdb_db = mdb_cli["project"]
    mdb_col = mdb_db["dataframes"]
    row_ins = { "dataframe":str(x_df.collect()), "schema":str(x_df.schema), "dthr":datetime.now()}
    if type(x_def_name) is str: row_ins['function'] = x_def_name
    if type(x_comment) is str: row_ins['comment'] = x_comment
    mdb_col.insert_one(row_ins)
    return


def dfGetMongo()->DataFrame:
    # Retorna último DataFrame armazenado no MongoDB
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
    # Define o tipo de saída
    # x_type = 'df', retona como objeto DataFrame
    # x_type = 'json', retorna com o dados do DataFrame em formato JSON
    # caso contrário retorna com o Schema do DataFrame em formato JSON
    res = ''
    if x_type == 'json': res = toJson(x_dataframe.toJSON().collect()) 
    elif x_type == 'df': res = x_dataframe
    else: res = x_dataframe.schema.json()
    return res


def toJson(x_json):
    # Função que converte objetos em JSON
    try:
        res = json.loads(x_json) 
    except:
        res = json.loads(json.dumps(x_json))
    return x_json

def lchar(x:str)->str:
    # Remove qualquer caractere diferente de "a" até "z" e traás somentes os 5 primeiros digitos
    return re.sub("[^a-z]", "", x)[:5]
