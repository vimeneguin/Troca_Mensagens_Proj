import os
import re
import json
import time
import zmq
import msgpack

# Configurações (os valores padrão servem para o docker compose)
nome = os.environ.get("SERVER_NAME", "servidor_1")
broker = os.environ.get("BROKER_HOST", "broker")
proxy = os.environ.get("PROXY_HOST", "proxy")
referencia = os.environ.get("REFERENCE_HOST", "referencia")
pasta_dados = os.environ.get("DADOS_DIR", "/dados")

os.makedirs(pasta_dados, exist_ok=True)
arquivo = os.path.join(pasta_dados, nome + ".json")

relogio = 0         # relógio lógico
deslocamento = 0.0  # ajuste do relógio físico (vem do serviço de referência)
rank = 0
mensagens_recebidas = 0


def agora():
    # hora física já com o ajuste da referência
    return time.time() + deslocamento


def receber_clock(conteudo):
    # relógio lógico: ao receber, fica com o maior valor
    global relogio
    relogio = max(relogio, conteudo.get("clock", 0))


def preparar(conteudo):
    # relógio lógico: incrementa antes de enviar e coloca na mensagem
    global relogio
    relogio += 1
    conteudo["timestamp"] = agora()
    conteudo["clock"] = relogio
    return conteudo


def carregar():
    # le os dados salvos em disco (se o arquivo existir)
    if os.path.exists(arquivo):
        with open(arquivo, "r") as f:
            dados = json.load(f)
    else:
        dados = {}
    dados.setdefault("logins", [])
    dados.setdefault("canais", [])
    dados.setdefault("mensagens", [])
    return dados


def salvar():
    with open(arquivo, "w") as f:
        json.dump(dados, f, indent=2)


dados = carregar()
usuarios_online = set()  # usuários logados nesta execução do servidor


def tratar_login(msg):
    usuario = msg.get("user", "").strip()
    if usuario == "":
        return {"status": "erro", "description": "nome de usuário vazio"}
    if len(usuario) > 20:
        return {"status": "erro", "description": "nome de usuário muito grande"}
    if usuario in usuarios_online:
        return {"status": "erro", "description": "usuário já está logado"}

    usuarios_online.add(usuario)
    dados["logins"].append({"user": usuario, "timestamp": msg["timestamp"]})
    salvar()
    return {"status": "sucesso"}


def tratar_criar_canal(msg):
    canal = msg.get("channel", "").strip()
    if canal == "":
        return {"status": "erro", "description": "nome do canal vazio"}
    if not re.fullmatch(r"[A-Za-z0-9_-]+", canal):
        return {"status": "erro", "description": "nome do canal só pode ter letras, números, _ e -"}
    if canal in dados["canais"]:
        return {"status": "erro", "description": "canal já existe"}

    dados["canais"].append(canal)
    salvar()
    return {"status": "sucesso"}


def tratar_listar_canais(msg):
    return {"status": "sucesso", "channels": dados["canais"]}


def tratar_publicar(msg):
    usuario = msg.get("user", "")
    canal = msg.get("channel", "")
    texto = msg.get("message", "")

    # os servidores ainda não replicam os canais entre si, então não dá para
    # exigir que o canal esteja na lista deste servidor (ele pode ter sido criado em outro)
    if not re.fullmatch(r"[A-Za-z0-9_-]+", canal):
        return {"status": "erro", "description": "nome de canal inválido"}
    if texto == "":
        return {"status": "erro", "description": "mensagem vazia"}

    # publica no canal usando o nome do canal como tópico
    conteudo = preparar({"user": usuario, "channel": canal, "message": texto})
    pub.send_multipart([canal.encode(), msgpack.packb({"service": "publish", "data": conteudo})])
    print(f"[{nome}] PUBLICADO | {canal} | {conteudo}", flush=True)

    # guarda a publicação em disco
    dados["mensagens"].append({
        "user": usuario,
        "channel": canal,
        "message": texto,
        "timestamp_requisicao": msg["timestamp"],
        "timestamp_publicacao": conteudo["timestamp"],
        "clock": conteudo["clock"],
    })
    salvar()
    return {"status": "sucesso"}


def pedir_referencia(servico, conteudo):
    # envia uma requisição para o serviço de referência e devolve a resposta
    conteudo = preparar(conteudo)
    print(f"[{nome}] ENVIADO   | referencia/{servico} | {conteudo}", flush=True)
    ref.send(msgpack.packb({"service": servico, "data": conteudo}))
    resposta = msgpack.unpackb(ref.recv())["data"]
    receber_clock(resposta)
    print(f"[{nome}] RECEBIDO  | referencia/{servico} | {resposta}", flush=True)
    return resposta


def heartbeat():
    global deslocamento
    t0 = time.time()
    resposta = pedir_referencia("heartbeat", {"user": nome})
    t1 = time.time()

    # atualiza o relógio: hora da referência + metade do tempo de ida e volta
    if resposta["status"] == "OK":
        hora_certa = resposta["time"] + (t1 - t0) / 2
        deslocamento = hora_certa - t1
        print(f"[{nome}] RELOGIO   | ajuste de {deslocamento:.6f}s", flush=True)

    lista = pedir_referencia("list", {"user": nome})
    print(f"[{nome}] SERVIDORES | {lista['list']}", flush=True)


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://" + broker + ":5556")

pub = context.socket(zmq.PUB)
pub.connect("tcp://" + proxy + ":5557")
time.sleep(1)  # tempo para a conexão com o proxy ficar pronta

ref = context.socket(zmq.REQ)
ref.connect("tcp://" + referencia + ":5559")

# pede o rank para o serviço de referência
rank = pedir_referencia("rank", {"user": nome})["rank"]
print(f"[{nome}] servidor iniciado com rank {rank}", flush=True)

while True:
    pacote = socket.recv()
    msg = msgpack.unpackb(pacote)
    servico = msg["service"]
    conteudo = msg["data"]
    receber_clock(conteudo)
    print(f"[{nome}] RECEBIDO  | {servico} | {conteudo}", flush=True)

    if servico == "login":
        resposta = tratar_login(conteudo)
    elif servico == "channel":
        resposta = tratar_criar_canal(conteudo)
    elif servico == "channels":
        resposta = tratar_listar_canais(conteudo)
    elif servico == "publish":
        resposta = tratar_publicar(conteudo)
    else:
        resposta = {"status": "erro", "description": "serviço desconhecido"}

    preparar(resposta)
    print(f"[{nome}] ENVIADO   | {servico} | {resposta}", flush=True)
    socket.send(msgpack.packb({"service": servico, "data": resposta}))

    # a cada 10 mensagens de clientes manda o heartbeat para a referência
    mensagens_recebidas += 1
    if mensagens_recebidas % 10 == 0:
        heartbeat()
