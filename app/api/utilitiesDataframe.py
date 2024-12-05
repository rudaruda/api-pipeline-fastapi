from datetime import datetime
import pymongo
from pyspark.sql import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from . import utilities 


# create a SparkSession
spark = SparkSession.builder.appName("ReadJSON").getOrCreate()


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


def dfOutput(x_type:str, x_dataframe:DataFrame, x_title:str='Retorno HTML', x_comment:str=None):
    # Define o tipo de saída
    # x_type = 'df', retona como objeto DataFrame
    # x_type = 'json', retorna dados do DataFrame em formato JSON
    # x_type = 'html', retorna dados do DataFrame em formato HTML 
    # caso contrário retorna com o Schema do DataFrame em formato JSON
    res = ''
    if x_type == 'json': res = utilities.toJson(x_dataframe.toJSON().collect()) 
    elif x_type == 'df': res = x_dataframe
    elif x_type == 'html': res = htmlReport(x_title, x_dataframe, x_comment)
    else: res = x_dataframe.schema.json()
    return res


def dfContactTwoCols(new_col:str, x_col1:str, x_col2:str, x_dataframe:DataFrame)->DataFrame:
    x_cols = x_dataframe.columns
    if x_col1 in x_cols and x_col2 in x_cols and not new_col in x_cols: 
        x_dataframe = x_dataframe.withColumn( new_col, F.concat( F.col(x_col1), F.lit(" "), F.col(x_col2)))
        dfToMongo(x_dataframe,'dfContactTwoCols','concatenou colunas')
        print(f'* concatenou: {new_col}={x_col1}+{x_col2}')
    return x_dataframe


def dfToString(df, n=20, truncate=False, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return(df._jdf.showString(n, 20, vertical))
    else:
        return(df._jdf.showString(n, int(truncate), vertical))


def htmlReport(x_title:str, x_dataframe:DataFrame, x_comment:str=None)->str:
    x_table = dfToString(x_dataframe)
    if x_title != 'only_table':
        x_html = """<html> <head> <style>
            .dcenter {display: grid; justify-content: center; text-align: center;}
            body { margin: 3em; margin-top: 1em;}
            h3 {font-family: monospace;}
            table, th, td { border: 1px solid;border-collapse:collapse; }
            table { width: 100%; }
            sub { color: solver; margin-top: 1em }
            </style></head><body><div class=dcenter>"""
        x_comment = x_comment is str and "<p>{x_comment}</p>" or ""
        x_tit_pre = (not "()" in x_title and "Relatório: " or "")
        x_html += f"<h3>{x_tit_pre}{x_title}</h3>{x_comment}<pre>{x_table}</pre>"
        x_html += "<br><sub>Relatório produzido com PySpark</sup></div></body></html>"
    else: x_html = x_table
    return x_html


