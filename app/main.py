from app.api import api, eventProcessor, utilities, aggregator, writer
from app.db.models import UserAnswer
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import ORJSONResponse
from starlette.responses import Response, HTMLResponse, JSONResponse, StreamingResponse, FileResponse


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


def jsonOrHtmlResponse(x_res, x_msg):
    # Define se o retorno será JSON ou HTML
    x_res = utilities.toJson(x_res)
    if not type(x_res) is str: 
        if not 'html' in str(x_res)[:7]:
            return JSONResponse(content=[{ "message": x_msg, "object": x_res }]) 
    return HTMLResponse(content=x_res)


@app.get("/eventprocessor/1-loadfile/{p:path}")
async def json_schema(p: str, req: Request):
    ## 1.1.1) Ler o JSON em um DataFrame PySpark.
    res = eventProcessor.dfJsonLoad( utilities.lchar(p+req.url.query) )
    return jsonOrHtmlResponse(res, "Arquivo 'input-data.json', carregado DataFrame SPARK")


@app.get("/eventprocessor/2-searchitemlist/{p:path}")
def json_searchitemlist(p: str, req: Request):
    ## 1.1.2) Explodir o array searchItemsList para normalizar os dados.
    res = eventProcessor.dfJsonSearchItemList( utilities.lchar(p+req.url.query) )
    # "Dados do atributo searchItemsList"
    return jsonOrHtmlResponse(res, "Limpeza do dado: dfJsonSearchItemList()")


@app.get("/eventprocessor/3-featurecols/{p:path}")
def json_featurecols(p: str, req: Request):
    ## 1.1.3) Criar colunas derivadas: 'departure_datetime', 'arrival_datetime' e 'route'
    res = eventProcessor.dfJsonFeatureCols( utilities.lchar(p+req.url.query))
    return jsonOrHtmlResponse(res, """Feature aplicada: 'departure_datetime', 'arrival_datetime', 'route'.
        * Somente quando as colunas depentes existem no DataFrame 
        ** Exemplo 'arrival_datetime' não esta no Schema pois 'arrivalDate' e 'arrivalHour' não existem neste DataFrame.""")


@app.get("/eventprocessor/4-futuredeparture/{p:path}")
def json_futuredeparture(p: str, req: Request):
    ## 1.1.4) Viagens futuras (baseadas no departure_datetime).
    res = eventProcessor.dfFilterDeparturesFutures( utilities.lchar(p+req.url.query) )
    return jsonOrHtmlResponse(res, "Filtro de viagens futuras, departure_datetime > NOW")


@app.get("/eventprocessor/5-availableseats/{p:path}")
def json_availableseats(p: str, req: Request):
    ## 1.1.4) Viagens com availableSeats > 0
    res = eventProcessor.dfFilterSeatsAvailables( utilities.lchar(p+req.url.query) )
    return jsonOrHtmlResponse(res, "Filtro de viagens com acentos disponíveis, availableSeats > 0")


@app.get("/agregator/6-avgrota/{p:path}", response_class=HTMLResponse)
async def report_avgrota(p: str, req: Request):
    ## 1.2.1) Calcular o preço médio por rota e classe de serviço.
    res = aggregator.dfGetPriceAvgRouteClasse( utilities.lchar(p+req.url.query) )
    return jsonOrHtmlResponse(res) 


@app.get("/agregator/7-availableseats/{p:path}", response_class=HTMLResponse)
async def report_availableseats(p: str, req: Request):
    ## 1.2.2) Determinar o total de assentos disponíveis por rota e companhia.
    return aggregator.dfGetTotalAvalSeatsRouteClasse( utilities.lchar(p+req.url.query) )


@app.get("/agregator/8-popularroute/{p:path}", response_class=HTMLResponse)
async def report_porpularroute(p: str, req: Request):
    ## 1.2.3) Identificar a rota mais popular por companhia de viagem.
    return aggregator.dfGetFrequenceRoute( utilities.lchar(p+req.url.query) )


@app.get("/eventprocessor/9-process_events/{p:path}")
def json_process_events(p: str, req: Request):
    ## 2.1) EventProcessor: ■ Leia o JSON. ■ Normalize os dados. ■ Retorne o DataFrame processado
    return [{ "message":"eventProcessor.process_events() -> EXECUTADO!"
             ,"object":eventProcessor.process_events( utilities.lchar(p+req.url.query) ) }]


@app.get("/aggregator/10-aggregate_data/{p:path}")
def json_process_events(p: str, req: Request):
    ## 2.2) Aggregator.aggregate_data(): ■ Receba o DataFrame processado. ■ Gere as agregações solicitadas. ■ Retorne um DataFrame com os insights
    return [{ "message":"aggregator.aggregate_data() -> EXECUTADO!"
             ,"object":aggregator.aggregate_data( utilities.lchar(p+req.url.query) ) }]


@app.get("/writer/write_data", response_class=FileResponse) #, response_description='zip'
async def writer_write_data():
    ## 2.3) Writer.write_data(): ■ Salve os dados processados em Parquet.
    response = StreamingResponse(writer.write_data(), media_type="application/x-zip-compressed")
    response.headers["Content-Disposition"] = "attachment; filename=dataframe_parquet.zip"


""""
http://127.0.0.1:8000/eventprocessor/1-loadfile

http://127.0.0.1:8000/eventprocessor/2-searchitemlist/

http://127.0.0.1:8000/eventprocessor/3-featurecols/

http://127.0.0.1:8000/eventprocessor/4-futuredeparture

http://127.0.0.1:8000/eventprocessor/5-availableseats

http://127.0.0.1:8000/aggregator/6-avgrota

http://127.0.0.1:8000/aggregator/7-availableseats

http://127.0.0.1:8000/aggregator/8-popularroute

http://127.0.0.1:8000/eventprocessor/10-process_events

http://127.0.0.1:8000/aggregator/11-aggregate_data

"""