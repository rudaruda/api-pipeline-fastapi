from app.api import eventProcessor, utilities, aggregator, writer
from app.db.models import UserAnswer
from fastapi import FastAPI, HTTPException, Request
from starlette.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse, RedirectResponse
import time

app = FastAPI()


@app.get("/")
def root():
    return RedirectResponse(url="/docs")
    #return {"message": "Fast API in Python"}


@app.post("/answer", status_code=201)
def create_answer(payload: UserAnswer):
    payload = payload.dict()
    return api.create_answer(payload)


@app.get("/eventprocessor/1-loadfile/{p:path}")
async def json_schema(p: str, req: Request):
    ## 1.1.1) Ler o JSON em um DataFrame PySpark.
    res = eventProcessor.dfJsonLoad( utilities.lchar(p+req.url.query) )
    return utilities.jsonOrHtmlResponse(res, "Arquivo 'input-data.json', carregado DataFrame SPARK")


@app.get("/eventprocessor/2-searchitemlist/{p:path}")
def json_searchitemlist(p: str, req: Request):
    ## 1.1.2) Explodir o array searchItemsList para normalizar os dados.
    res = eventProcessor.dfJsonSearchItemList( utilities.lchar(p+req.url.query) )
    # "Dados do atributo searchItemsList"
    return utilities.jsonOrHtmlResponse(res, "Limpeza do dado: dfJsonSearchItemList()")


@app.get("/eventprocessor/3-featurecols/{p:path}")
def json_featurecols(p: str, req: Request):
    ## 1.1.3) Criar colunas derivadas: 'departure_datetime', 'arrival_datetime' e 'route'
    res = eventProcessor.dfJsonFeatureCols( utilities.lchar(p+req.url.query))
    return utilities.jsonOrHtmlResponse(res, """Feature aplicada: 'departure_datetime', 'arrival_datetime', 'route'.
        * Somente quando as colunas depentes existem no DataFrame 
        ** Exemplo 'arrival_datetime' não esta no Schema pois 'arrivalDate' e 'arrivalHour' não existem neste DataFrame.""")


@app.get("/eventprocessor/4-futuredeparture/{p:path}")
def json_futuredeparture(p: str, req: Request):
    ## 1.1.4) Viagens futuras (baseadas no departure_datetime).
    res = eventProcessor.dfFilterDeparturesFutures( utilities.lchar(p+req.url.query) )
    return utilities.jsonOrHtmlResponse(res, "Filtro de viagens futuras, departure_datetime > NOW")


@app.get("/eventprocessor/5-availableseats/{p:path}")
def json_availableseats(p: str, req: Request):
    ## 1.1.4) Viagens com availableSeats > 0
    res = eventProcessor.dfFilterSeatsAvailables( utilities.lchar(p+req.url.query) )
    return utilities.jsonOrHtmlResponse(res, "Filtro de viagens com acentos disponíveis, availableSeats > 0")


@app.get("/agregator/6-avgrota/{p:path}", response_class=HTMLResponse)
async def report_avgrota(p: str, req: Request):
    ## 1.2.1) Calcular o preço médio por rota e classe de serviço.
    res = aggregator.dfGetPriceAvgRouteClasse( utilities.lchar(p+req.url.query) )
    return utilities.jsonOrHtmlResponse(res) 


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
    return response

@app.get("/writer/write_data/download", response_class=FileResponse) #, response_description='zip'
async def writer_write_data():
    ## 2.3) Writer.write_data(): ■ Salve os dados processados em Parquet.
    response = StreamingResponse(writer.write_data(), media_type="application/x-zip-compressed")
    response.headers["Content-Disposition"] = "attachment; filename=dataframe_parquet.zip"
    return response

@app.get("/writer/write_data")
async def prepare_download():
    ## 2.3) Writer.write_data(): Salve os dados processados em Parquet.
    html_content = """
    <html>
        <head>
            <title>Download do Parquet</title>
        </head>
        <body>
            <h1>Preparando o arquivo para download...</h1>
            <p>O download começará em instantes.</p>
            <p>O arquivo esta zipado, basta descompactar pra visualizar o conteudo de arquivos parquet.<br>Reforço que esta é apenas uma solução paliativa para completar o objetivo<br>Que nós provoca, sobre o que mais podemos fazer. Num ambiente produtivo teriamos outras alternativas,<br>compartilhando recursos de rede e automatizando mais processo.</p>
            <script>
                setTimeout(function() {
                    window.location.href = '/writer/write_data/download';
                }, 2000);
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/pipeline", response_class=HTMLResponse)
async def run_pipeline():
    ## 3) Parte 3: Testar o Pipeline
    # Instancie as classes EventProcessor, Aggregator e Writer.
    return StreamingResponse(eventProcessor.runPipeline(), media_type="text/html")


@app.get("/insights", response_class=HTMLResponse)
async def run_insights():
    ## 2.2) Retorne um DataFrame com os insights.
    # ■ Receba o DataFrame processado. ■ Gere as agregações solicitadas. ■ Retorne um DataFrame com os insights.
    return StreamingResponse(aggregator.runInsights(), media_type="text/html")

