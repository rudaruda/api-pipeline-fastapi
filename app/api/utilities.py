import json, re


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
