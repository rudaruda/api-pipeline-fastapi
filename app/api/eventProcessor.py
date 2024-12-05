from datetime import datetime
import json
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from . import utilitiesDataframe
import time


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
    return utilitiesDataframe.dfOutput(p, df, "Carregou arquivo JSON: dfJsonLoad()", f"{x_path}")


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
    return utilitiesDataframe.dfOutput(p, df, "Limpeza do dado: dfJsonSearchItemList()", "Conteúdo do atributo 'searchItemsList'")


def dfJsonFeatureCols(p:str,x_dataframe:DataFrame=None):
    print('*  Running... dfJsonFeatureCols()')
    ## 1.3) Criar colunas derivadas: 'departure_datetime', 'arrival_datetime' e 'route'
    # ADD new columns: departure_datetime, arrival_datetime, route
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    df = utilitiesDataframe.dfContactTwoCols("departure_datetime", "departureDate", "departureHour", df)
    df = utilitiesDataframe.dfContactTwoCols("arrival_datetime", "arrivalDate", "arrivalHour", df)
    df = utilitiesDataframe.dfContactTwoCols("route", "originCity", "destinationCity", df)
    print('*  Finish! dfJsonFeatureCols()')
    return utilitiesDataframe.dfOutput(p, df, "Enriquecimento do dado: dfJsonFeatureCols()","'departure_datetime', 'arrival_datetime' e 'route'")


def dfFilterDeparturesFutures(p:str,x_dataframe:DataFrame=None):
    print('*  Running... dfFilterDeparturesFutures()')
    ## 1.4) Viagens futuras (baseadas no departure_datetime).
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    # Conversão da coluna 'departure_datetime' para timestamp:
    #df = df.withColumn("departure_datetime", F.to_datetime( F.col("departure_datetime"), 'yyyy-MM-dd HH:mm:ss') )
    # Query 'departure_datetime' > NOW
    df = df.filter(F.col("departure_datetime") > F.current_timestamp())
    utilitiesDataframe.dfToMongo(df,'dfFilterSeatsAvailables',f'Conversão da coluna "departure_datetime" para Datetime e filtro departure_datetime > NOW')
    print('*  Finish! dfFilterDeparturesFutures()')
    return utilitiesDataframe.dfOutput(p, df, "Filtro do dado: dfFilterDeparturesFutures()", "WHERE 'departure_datetime' > NOW")


def dfFilterSeatsAvailables(p:str,x_dataframe:DataFrame=None):
    print('*  Running... dfFilterSeatsAvailables()')
    ## 1.5) Viagens com availableSeats > 0
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    # Conversão da coluna 'availableSeats' integer: 
    df = df.withColumn("availableSeats", F.col("availableSeats").cast("integer"))
    # Query 'availableSeats' > 0
    df = df.filter(F.col("availableSeats") > 0)
    utilitiesDataframe.dfToMongo(df,'dfFilterSeatsAvailables',f'Conversão da coluna "availableSeats" para Integer e filtro availableSeats > 0')
    print('*  Finish! dfFilterSeatsAvailables()')
    return utilitiesDataframe.dfOutput(p, df, "Filro do dado: dfFilterSeatsAvailables()", "WHERE 'availableSeats' > 0")


def process_events():
    print('** Running... process_events()')
    ## 2.1) EventProcessor: ■ Leia o JSON. ■ Normalize os dados. ■ Retorne o DataFrame processado
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


def runPipeline():
    ## 3) Parte 3: Testar o Pipeline
    # Instancie as classes EventProcessor, Aggregator e Writer.
    total_steps = 7
    yield """
    <html>
    <head>
        <style> h2{position:fixed;top:6px;z-index:1001;text-align:center;width: calc(100% - 282px);} #download-btn1{bottom: 50%;background-color: forestgreen;}
        body{font-family:Arial,sans-serif;margin:0;padding:0;background-color:#f4f4f9;margin-left:230px}.progress-bar-container{position:fixed;top:0;right:0;width:calc(100% - 260px);background:#ddd;height:40px;z-index:1000;box-shadow:0 -2px 6px 0px}.progress-bar{height:100%;width:0%;background-color:#007bff;transition:width 0.2s ease}.logs-container{margin-top:40px;padding:20px;max-width:800px;margin-left:auto;margin-right:auto;text-align:center}pre{background:#272822;color:#f8f8f2;padding:10px;border-radius:5px;overflow-x:auto;font-size:14px;text-align:left}button{position:fixed;bottom:15%;left:calc(50% + 70px);transform:translateX(-50%);background-color:#007bff;border:none;color:#fff;padding:12px 30px;text-align:center;text-decoration:none;font-size:16px;cursor:pointer;border-radius:5px;box-shadow:0 4px 6px rgb(0 0 0 / .2);transition:all 2s ease;opacity:0;transition:opacity 2s ease-in-out;zoom:140%;text-shadow:black 1px 1px 3px}button:hover{background-color:#0056b3;box-shadow:0 6px 10px rgb(0 0 0 / .3)}button:active{box-shadow:0 2px 4px rgb(0 0 0 / .1);}.show-button{opacity:1}.sidebar{position:fixed;top:50%;left:0;background-color:#333;color:#fff;padding:20px;width:220px;box-shadow:2px 0 5px rgb(0 0 0 / .1);z-index:1002;font-size:14px;transform:translateY(-50%);margin-top: 1.4%; padding-bottom: 14%; min-height: 400px; padding-top: 25%; }.sidebar img{width:50px;height:50px;border-radius:50%;margin-bottom:20px}.sidebar a{color:#fff;text-decoration:none;margin:5px 0;display:block;font-size:14px}.sidebar a:hover{text-decoration:underline}

         </style>
    </head>
    <script>
            document.addEventListener('DOMContentLoaded', function() {
                const elements = document.querySelectorAll('pre');
                elements.forEach(element => {
                    element.addEventListener('click', function() {
                        element.classList.add('popup');
                        const overlay = document.createElement('div');
                        overlay.className = 'overlay';
                        document.body.appendChild(overlay);
                        overlay.addEventListener('click', function() {
                            element.classList.remove('popup');
                            document.body.removeChild(overlay);
                        });
                    });
                });
            });
        </script>
    <body>
        <h2>Pipeline</h2>
        <div class="sidebar">
            <img src="https://via.placeholder.com/50" alt="Minha Foto">
            <h1>Filipe Rudá</h1>
            <h3>Contato</h3>
            <p>Email: filiperuda@gmail.com
            <a href="https://www.linkedin.com/in/filiperuda/" target="_blank">linkedin.com/in/filiperuda</a>
            </p>
            
            <a href="https://github.com/rudaruda/api-pipeline-fastapi" target="_blank">github.com/rudaruda/<br>api-pipeline-fastapi</a>
            <a href="http://localhost:8000/docs" target="_blank">Documentação<br>localhost:8080</a>
        </div>
        <div class="progress-bar-container">
            <div class="progress-bar" id="progress-bar"></div>
        </div>
        <script>
    document.addEventListener("DOMContentLoaded", function () {
        // Define o timeout em milissegundos (exemplo: 30 segundos)
        const TIMEOUT_MS = 30000; 

        // Inicia o temporizador
        const timeout = setTimeout(function () {
            alert("O carregamento está demorando mais do que o esperado.\nPor favor, verifique se o serviço do MongoDB esta ativo ou se há memória disponível para a aplicação");
        }, TIMEOUT_MS);

        // Função para sinalizar conclusão de carregamento
        function markLoadingComplete() {
            clearTimeout(timeout); // Cancela o timeout
        }

        // Simula a conclusão de um processo (pode ser ajustado ao seu fluxo)
        // Aqui utilizamos o final do log como exemplo de carregamento concluído
        const logs = document.getElementById("logs");
        const observer = new MutationObserver(function (mutationsList) {
            for (let mutation of mutationsList) {
                if (logs.textContent.includes("Processando etapa 12/12...")) {
                    markLoadingComplete();
                }
            }
        });

        // Observa mudanças no elemento de logs
        observer.observe(logs, { childList: true, subtree: true });
    });
</script>
        
        <div class="logs-container">
            <pre id="logs">
    """
    # Ler arquivo JSON
    progress = int((0 / total_steps) * 100)
    yield f"....\n"
    df = dfJsonLoad('')
    progress = int((1 / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    yield f"> Processando etapa {1}/{total_steps}...\n"
    yield f"> Carregamdo arquivo JSON 'input_data.json' para DataFrame SPARK\n"
    #table = utilitiesDataframe.htmlReport('only_table',df)
    #yield f"{table}'\n"

    # x Normalize os dados
    # Seleção do atributo 'searchItemList'
    df = dfJsonSearchItemList('df',df)
    progress = int((2 / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    yield f"Processando etapa {2}/{total_steps}...\n"
    yield f"> Normalizando dados... dfJsonSearchItemList()\n"
    yield f"> Seleção de atributo 'searchItemList'\n"
    table = utilitiesDataframe.htmlReport('only_table',df)
    yield f"{table}'\n"
    time.sleep(3)
    yield f"...\n"
    time.sleep(2)
    
    # Enriquecimento de colunas 'departure_datetime', 'arrival_datetime' e 'route'
    df = dfJsonFeatureCols('df',df)
    progress = int((3 / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    yield f"Processando etapa {3}/{total_steps}...\n"
    yield f"> Normalizando dados... dfJsonFeatureCols()\n"
    yield f"> Enriquecendo com colunas 'departure_datetime', 'arrival_datetime' e 'route'\n"
    table = utilitiesDataframe.htmlReport('only_table',df)
    yield f"{table}'\n"
    time.sleep(3)
    yield f"...\n"
    time.sleep(2)

    # Filtro somente partidas futuras
    df = dfFilterDeparturesFutures('df',df)
    progress = int((4 / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    yield f"Processando etapa {4}/{total_steps}...\n"
    yield f"Normalizando dados... dfFilterDeparturesFutures()\n"
    yield f"> Filtro somente partidas futuras, dfFilterDeparturesFutures()\n"
    yield f"> WHERE 'departure_datetime > NOW\n"
    table = utilitiesDataframe.htmlReport('only_table',df)
    yield f"{table}'\n"
    time.sleep(3)
    yield f"...\n"
    time.sleep(2)

    # Filtro somente assentos disponiveis
    df = dfFilterSeatsAvailables('df',df)
    progress = int(((5) / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    yield f"Processando etapa {5}/{total_steps}...\n"
    yield f"> Normalizando dados... dfFilterDeparturesFutures()\n"
    yield f"> Filtro somente assentos disponíveis, dfFilterSeatsAvailables()\n"
    yield f"> WHERE 'availableSeats' > 0\n"
    #■ Retorne o DataFrame processado
    table = utilitiesDataframe.htmlReport('only_table',df)
    yield f"{table}'\n"
    time.sleep(3)
    yield f"...\n"
    time.sleep(2)

    progress = int(((6) / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    yield f"\n"
    yield f"> Persistindo dataframe no MongoDB\n"

    # Gravano no MongoDB
    utilitiesDataframe.dfToMongo(df,'process_events()','carregou JSON, normalizou e retornou DataFrame')

    #Concluído
    time.sleep(3) 
    yield f"\n...\n"
    time.sleep(2.5)
    yield f"\n....\n"
    time.sleep(1.5)
    yield f"\n"
    yield f"** PROCESSAMENTO CONCLUÍDO **\n"
    progress = int(((7) / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    
    # Botão de download
    yield """
            </pre>
            <button id="download-btn" class="show-button" onclick="location.href='/writer/write_data'" style="opacity:0">Download parquet files</button>
            <button id="download-btn1" class="show-button" onclick="location.href='/insights'" style="opacity:0">Visualizar analises</button>
        """""
    yield f"<script>document.getElementById('download-btn1').style.opacity = 1;</script>\n"
    yield f"<script>document.getElementById('download-btn').style.opacity = 1;</script>\n"
    yield """
        </div>
    </body>
    </html>
    """

    # onclick="location.href='pageurl.html';"