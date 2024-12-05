import sys

import json
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, sum
from . import utilitiesDataframe 
from . import eventProcessor

# create a SparkSession
spark = SparkSession.builder.appName("ReadJSON").getOrCreate()


def dfGetPriceAvgRouteClasse(p:str=None,x_dataframe:DataFrame=None):
    print('*  Running... dfGetPriceAvgRouteClasse()')
    ## 2.1) Calcular o preço médio por rota e classe de serviço. 
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    x_col_group = ["route","serviceClass"]
    if p is None or p == '': p = 'html'
    if not "route" in df.columns: x_col_group.remove("route")
    df = df.groupBy(x_col_group).agg(avg("price").alias("AVG Price")).orderBy(x_col_group)
    print('*  Finish! dfGetPriceAvgRouteClasse()')
    return utilitiesDataframe.dfOutput(p, df, "Média de preço")


def dfGetTotalAvalSeatsRouteClasse(p:str=None,x_dataframe:DataFrame=None):
    print('*  Running... dfGetTotalAvalSeatsRouteClasse()')
    ## 2.2) Determinar o total de assentos disponíveis por rota e companhia.
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    x_col_group = ["route","travelCompanyName"]
    if not "route" in df.columns: x_col_group.remove("route")
    df = df.groupBy(x_col_group).agg(sum("availableSeats").alias("Total Available Seats")).orderBy(x_col_group)
    print('*  Finish! dfGetTotalAvalSeatsRouteClasse()')
    return utilitiesDataframe.dfOutput(p, df, "Total de assentos disponíveis")


def dfGetFrequenceRoute(p:str=None,x_dataframe:DataFrame=None):
    print('*  Running... dfGetFrequenceRoute()')
    ## 2.3) Identificar a rota mais popular por companhia de viagem.
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    x_col_group = ["route","travelCompanyName"]
    if not "route" in df.columns: x_col_group.remove("route")
    df = df.groupBy(x_col_group).agg(sum("availableSeats").alias("Total Available Seats")).orderBy("availableSeats", ascending=False)
    print('*  Finish! dfGetFrequenceRoute()')
    return utilitiesDataframe.dfOutput(p, df, "Rota mais popular")


def aggregate_data():
    print('** Running... aggregate_data()')
    ## Aggregator.aggregate_data(): ■ Receba o DataFrame processado. ■ Gere as agregações solicitadas. ■ Retorne um DataFrame com os insights
    # ■ Receba o DataFrame processado
    df = eventProcessor.process_events()
    # ■ Gere as agregações solicitadas.
    df1 = dfGetPriceAvgRouteClasse('html',df)
    df2 = dfGetTotalAvalSeatsRouteClasse('html',df)
    df3 = dfGetFrequenceRoute('html',df)
    # ■ Retorne um DataFrame com os insights
    # Construir retorno
    print('** Finish! aggregate_data()')
    return