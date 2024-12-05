import json, re
from starlette.responses import HTMLResponse, JSONResponse


def toJson(x_json):
    # Função que converte objetos em JSON
    try:
        res = json.loads(x_json) 
    except:
        res = json.loads(json.dumps(x_json))
    return res


def lchar(x:str)->str:
    # Remove qualquer caractere diferente de "a" até "z" e traás somentes os 5 primeiros digitos
    return re.sub("[^a-z]", "", x)[:5]


def jsonOrHtmlResponse(x_res, x_msg):
    # Define se o retorno será JSON ou HTML
    x_res = toJson(x_res)
    if not type(x_res) is str: 
        if not 'html' in str(x_res)[:7]:
            return JSONResponse(content=[{ "message": x_msg, "object": x_res }]) 
    return HTMLResponse(content=x_res)
