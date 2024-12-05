from datetime import datetime
from . import eventProcessor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, sum, count, lit
from . import utilitiesDataframe 
import time


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
    df = df.groupBy(x_col_group).agg(count(lit(1)).alias("Qtd")).orderBy("Qtd", ascending=False)
    print('*  Finish! dfGetFrequenceRoute()')
    return utilitiesDataframe.dfOutput(p, df, "Rota mais popular")


def aggregate_data():
    print('** Running... aggregate_data()')
    ## 2.2) Aggregator.aggregate_data(): ■ Receba o DataFrame processado. ■ Gere as agregações solicitadas. ■ Retorne um DataFrame com os insights
    # ■ Receba o DataFrame processado
    df = eventProcessor.process_events()
    # ■ Gere as agregações solicitadas.
    df1 = dfGetPriceAvgRouteClasse('html',df)
    df2 = dfGetTotalAvalSeatsRouteClasse('html',df)
    df3 = dfGetFrequenceRoute('html',df)
    # ■ Retorne um DataFrame com os insights
    # Esta aqui -> runInsigts()
    print('** Finish! aggregate_data()')
    return


def runInsights():
    ## 2.2) Retorne um DataFrame com os insights.
    # ■ Receba o DataFrame processado. ■ Gere as agregações solicitadas. ■ Retorne um DataFrame com os insights.
    total_steps = 4
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
    # Relatório Média de Preço por Rota e Classe
    progress = int((0 / total_steps) * 100)
    yield f"....\n"
    df = utilitiesDataframe.dfGetMongo()
    df1 = dfGetPriceAvgRouteClasse('df', df)
    progress = int((1 / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    yield f"{1}) Média de preço por Rota e Classe\n"
    table = utilitiesDataframe.htmlReport('only_table',df1)
    yield f"{table}'\n"

    # Relatório de Assentos Disponíveis
    df2 = dfGetTotalAvalSeatsRouteClasse('df',df)
    progress = int((2 / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    yield f"{2}) Assentos disponíveis por classe\n"
    table = utilitiesDataframe.htmlReport('only_table',df2)
    yield f"{table}'\n"
    
    # Relatório Rota Mais Frequente
    df3 = dfGetFrequenceRoute('df',df)
    progress = int((3 / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    yield f"{3}) Rota mais popular\n"
    table = utilitiesDataframe.htmlReport('only_table',df3)
    yield f"{table}'\n"
    yield f"...'\n"
    yield f"Aqui podemos verificar que CURITIBA JOINVILE\n"
    yield f"é a que possui mais assentos disponíveis e que também é a mes'\n"
    yield f"que possui menor valor de passagem.'\n"
    yield f"Por outro lado BELO HORIZONTE BRASILIA é que possui maior valor de\n"
    yield f"passagem e ao mesmo tempo possui menor quantidade de assentos disponíveis.'\n"
    yield f"Nesse dataset não existe rota mais porpular, todas elas são equivalentes'\n"
    yield f"entre si, é um argumento plausivel para não dizer que para esse tipo\n"
    yield f"de analise seria necessário maior volume de regisros.\n"
    yield f"...\n"


    #Concluído
    time.sleep(7) 
    yield f" "
    yield f" "
    yield f"** PROCESSAMENTO CONCLUÍDO **\n"
    progress = int(((4) / total_steps) * 100)
    yield f"<script>document.getElementById('progress-bar').style.width = '{progress}%';</script>\n"
    
    # Botão de download
    yield """
            </pre>
            <button id="download-btn" class="show-button" onclick="location.href='/writer/write_data'" style="opacity:0">Download parquet files</button>
            <!-- <button id="download-btn1" class="show-button" onclick="location.href='/writer/write_data'" style="opacity:0">Visualizar analises</button> -->
        """""
    yield f"<script>document.getElementById('download-btn1').style.opacity = 1;</script>\n"
    #yield f"<script>document.getElementById('download-btn').style.opacity = 1;</script>\n"
    yield """
        </div>
    </body>
    </html>
    """