from fastapi import FastAPI, HTTPException, Request
from starlette.responses import Response
import re
from app.db.models import UserAnswer
from app.api import api, eventProcessor, utilities


app = FastAPI()


@app.get("/")
def root():
    return {"message": "Fast API in Python"}


@app.get("/user")
def read_user():
    return api.read_user()


@app.get("/question/{position}", status_code=200)
def read_questions(position: int, response: Response):
    question = api.read_questions(position)
    if not question:
        raise HTTPException(status_code=400, detail="Error")
    return question

@app.get("/alternatives/{question_id}")
def read_alternatives(question_id: int):
    return api.read_alternatives(question_id)

@app.post("/answer", status_code=201)
def create_answer(payload: UserAnswer):
    payload = payload.dict()
    return api.create_answer(payload)


@app.get("/result/{user_id}")
def read_result(user_id: int):

    return api.read_result(user_id)


@app.get("/json/schema/{p:path}")
def json_schema(p: str, req: Request):
    ## 1.1) Ler o JSON em um DataFrame PySpark.
    return [{ "message":"Arquivo 'input-data.json': Carregado no DataFrame SPARK"
             ,"object":eventProcessor.dfJsonSchema( lchar(p+req.url.query) ) }]


@app.get("/json/searchitemlist/{p:path}")
def json_searchitemlist(p: str, req: Request):
    ## 1.2) Explodir o array searchItemsList para normalizar os dados.
    return [{ "message":"Dados do atributo searchItemsList"
             ,"object":eventProcessor.dfJsonSearchItemList( lchar(p+req.url.query) ) }]


@app.get("/json/featurecols/{p:path}")
def json_featurecols(p: str, req: Request):
    ## 1.3) Criar colunas derivadas: 'departure_datetime', 'arrival_datetime' e 'route'
    return [{ "message":"""Feature aplicada: 'departure_datetime', 'arrival_datetime', 'route'.
              * Somente quando as colunas depentes existem no DataFrame 
             ** Exemplo 'arrival_datetime' não esta no Schema pois 'arrivalDate' e 'arrivalHour' não existem neste DataFrame."""
             ,"object":eventProcessor.dfJsonFeatureCols( lchar(p+req.url.query) ) }]

# {"message": "File 'input-data.json': Loaded to DataFrame SPARK"}

@app.get("/proxy/{p:path}")
def read_item(p: str, req: Request):
    return lchar(p+req.url.query)