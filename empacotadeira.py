import socket
import time
from umodbus import conf
from umodbus.client import tcp
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import datetime as dt
from bson.objectid import ObjectId
import variaveisempacotadeira

URL = "mongodb+srv://actual:actual@jera-opx2u.mongodb.net/sowelo?retryWrites=true&w=majority"

# URL = "mongodb://jera02:actual4941@mongodb.jera.agr.br/jera02"
#URL = "mongodb://localhost/sowelo"

condicao = True
ligado = False
dosou = False
mordeu = False
alarme_ativado = False

# variaveis de conexão com o clp
cliente = MongoClient(URL)
# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# sock.connect(('localhost', 502))
#sock.connect(('192.168.0.10', 502))
clp_conectado = False
sock = 1
operador = 0
peso = 0

data_e_hora_atuais = datetime.now()
contador = 0
contador_geral = 0
contador_embalagens = 0
contador_mordente = 0
embalagens_totais = 0
contador_tempo_processo = 0
cod_embalagem = 0
desliga = 3
usuario_ativo = ObjectId("5e3305df44faf350b03c87b0")
funcionario = ""
embalagem = ""

id_pacotes_dia = ObjectId("5e347312063680e3e1e6bcac")

id_empacotadeira_ativa = ObjectId("5e3307e844faf350b03c87b6")
id_sistema_moagem = ObjectId("5e3306f644faf350b03c87b5")


# variaveis para configurar os endereços do CLP
# COILS - 0 ou 1
clp_empac_onoff = 46000
clp_alarme = 46001
clp_mordente = 46002
clp_dosador = 46003


# variaveis para configurar os endereços do CLP
# HOlDERS REGISTERS - 0 ou 32000
clp_peso = 8010
clp_cod_embalagem = 8000
clp_operador = 8001

def conecta_clp():
    global clp_conectado
    global sock

    if(clp_conectado == False):
        try:    
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('localhost', 502))
            #sock.connect(('192.168.0.5', 502))
            clp_conectado = True
            print("Conexão Estabelecida")
            time.sleep(2)            
        except:
            print("Tentando Conectar com o CLP")
    if(clp_conectado == True):
        print("Maquina Conectada - Pronta pra Uso")


def rotinaprincipal():
   try:
        global ligado, dosou, mordeu, alarme_ativado
        global cod_embalagem
        global clp_conectado
        global sock, operador, peso
        global contador,contador_embalagens, contador_tempo_processo, embalagens_totais, contador_mordente,contador_geral

        message = tcp.read_coils(slave_id=1, starting_address=clp_empac_onoff,quantity=1)    
        response = tcp.send_message(message, sock)
        ligou = response[0]

        leitura_dosador = tcp.read_coils(slave_id=1, starting_address=clp_dosador,quantity=1)    
        res_dosador = tcp.send_message(leitura_dosador, sock)
        dosador = res_dosador[0]

        leitura_alarme = tcp.read_coils(slave_id=1, starting_address=clp_alarme,quantity=1)    
        res_alarme = tcp.send_message(leitura_alarme, sock)
        alarme = res_alarme[0]

        leitura_mordente = tcp.read_coils(slave_id=1, starting_address=clp_mordente,quantity=1)    
        res_mordente = tcp.send_message(leitura_mordente, sock)
        mordente = res_mordente[0]

        leitura_peso = tcp.read_holding_registers(slave_id=1, starting_address=clp_peso,quantity=1)    
        res_peso = tcp.send_message(leitura_peso, sock)
        peso = res_peso[0]

        leitura_cod_embalagem = tcp.read_holding_registers(slave_id=1, starting_address=clp_cod_embalagem,quantity=1)    
        res_cod_embalagem = tcp.send_message(leitura_cod_embalagem, sock)
        cod_embalagem = res_cod_embalagem[0]

        leitura_operador = tcp.read_holding_registers(slave_id=1, starting_address=clp_operador,quantity=1)
        res_operador = tcp.send_message(leitura_operador, sock)
        operador = res_operador[0]

        if(ligado == False):
            desligou_empac_status()
            desativou_alarme_status()
        

        print('Lendo Informações CLP' , contador )
        if (ligou == 1 and ligado == False ):    
            print('ligou Empacotadeira')
            ligou_empacotadeira()          
            ligado = True 
            contador = 0
            contador_embalagens = 0
            contador_tempo_processo = 0
            ligou_empac_status()
            contador_geral = 0
            atualizar_valores()
        elif(ligou == 0 and ligado == True):
            desligou_empacotadeira()
            print("Embalagens Totais", embalagens_totais)
            print("Embalagens Vazias", contador_mordente - embalagens_totais)
            atualiza_pacotes_dia()
            atualiza_sistema_moagem()
            ligado = False
            desligou_empac_status() 
            embalagens_totais = 0
            contador_mordente = 0
        if (alarme == 1 and alarme_ativado == False ):    
            print('Entrou em Alarme')
            hora_inicio_alarme()      
            alarme_ativado = True
            ativou_alarme_status()      
        elif(alarme == 0 and alarme_ativado == True):      
            print('Saiu do Alarme' , contador / 2)                
            alarme_ativado = False
            desativou_alarme_status()
            hora_fim_alarme()

        if(dosador == 1  and dosou == False):
            print("Dosou")
            contador_embalagens+=1
            embalagens_totais +=1
            dosou = True 

        elif (dosador == 1 and dosou == True):
            print("Dosagem já feita")
        if(dosador == 0 and dosou == True):
            dosou =  False
            print("dosagem parada")
    
        if(mordente == 1  and mordeu == False):
            print("Fechou mordente")
            contador_mordente+=1        
            mordeu = True        
        elif (mordente == 1 and mordeu == True):
            print("Mordente ativado")   

        if(mordente == 0 and mordeu == True):
            mordeu =  False
            print("Abriu Mordente")

        contador_tempo_processo = contador
        if(contador_tempo_processo == 30):
            print("1 min - Foram embaladas ",  contador_embalagens )
            print("peso embalado" ,  contador_embalagens * peso)
            atualiza_pacotes_dia()
            atualiza_sistema_moagem()
            contador = 0
            contador_embalagens = 0                
        time.sleep(1.0)
   except:
        print("Erro ao ler variaveis do CLP")
        clp_conectado = False

def ligou_empacotadeira():
    try:
        global data_ligou
        global cod_embalagem, operador,embalagem
        data_ligou = dt.datetime.now()
        busca_nome_funcionario()
        busca_embalagem()
        global id_empacotadeira
        global id_empacotadeira_ativa
        
        #blend é torrador de blend
        post = { "data_ligou" : data_ligou.strftime('%Y-%m-%d'),
                "usuario" : usuario_ativo,
                'hora_inicio' : data_ligou.strftime('%H:%M'),
                'embalagem' : embalagem,
                "operador" : funcionario,
                'empacotadeira' : id_empacotadeira_ativa

                }
        posts = cliente.sowelo.onoffempacotadeiras
        id_empacotadeira = posts.insert_one(post).inserted_id
    except:
        print("Erro ao salvar dados de quando ligou empacotadeira")

def busca_nome_funcionario():
    global operador
    global funcionario
    colecao_funcionarios = cliente.sowelo.funcionarios
    docfuncionarios = colecao_funcionarios.find_one({"cod_funcionario" : operador})
    funcionario = docfuncionarios['_id']

def busca_embalagem():
    global cod_embalagem
    global embalagem
    colecao_embalagens = cliente.sowelo.embalagens
    docembalagens = colecao_embalagens.find_one({"cod_clp" : cod_embalagem})
    embalagem = docembalagens['_id']

def atualiza_embalagem():
    global cod_embalagem
    global embalagem
    global embalagens_totais
    global peso
    print('Atualiza Embalagem')
    colecao_embalagens = cliente.sowelo.embalagens
    docembalagens = colecao_embalagens.find_one({"cod_clp" : cod_embalagem})
    embalagem = docembalagens['_id']
    embalagens_atuais = docembalagens['embalagens_totais']
    estoque_atual = docembalagens['peso_estoque']
    print(embalagem)

    colecao_embalagens.find_one_and_update(
            {"_id" : ObjectId(embalagem)},
            {'$set':
                {'embalagens_totais': embalagens_atuais + embalagens_totais ,
                'peso_estoque':estoque_atual + (embalagens_totais * peso),
                }           
            },upsert=True
        )

def desligou_empacotadeira():
    # try:
        global id_empacotadeira  
        data_desligou = dt.datetime.now()  
        global data_ligou
        global tempo_ligado
        global peso, contador_mordente, embalagens_totais
        atualiza_embalagem()
        colecao = cliente.sowelo.onoffempacotadeiras  
        doc = colecao.find_one({"_id" : id_empacotadeira})
        colecao.find_one_and_update(
            {"_id" : id_empacotadeira},
            {'$set':
                {'data_desligou': data_desligou.strftime('%Y-%m-%d'),
                'tempo_ligado': contador_geral,
                'hora_fim':data_desligou.strftime('%H:%M'),
                'peso' : embalagens_totais * peso,
                'embalagens_ok' :  embalagens_totais,
                'embalagens_perdidas' : contador_mordente - embalagens_totais}           
            },upsert=True
        )
   # except:
     #   print("Erro ao salvar dados de quando desligou empacotadeira")


def hora_inicio_alarme():
    try:
        global id_empacotadeira  
        hora_alarme_inicio = dt.datetime.now()  
        colecao = cliente.sowelo.onoffempacotadeiras   
        doc = colecao.find_one({"_id" : id_empacotadeira})
        colecao.find_one_and_update(
            {"_id" : id_empacotadeira},
            {'$set':
                {'hora_inicio_alarme':hora_alarme_inicio.strftime('%H:%M')
                }           
            },upsert=True
        )
    except:
        print("Erro ao salvar dados de quando desligou empacotadeira")


def hora_fim_alarme():
    try:
        global id_empacotadeira  
        hora_alarme_fim = dt.datetime.now()  
        colecao = cliente.sowelo.onoffempacotadeiras   
        doc = colecao.find_one({"_id" : id_empacotadeira})
        colecao.find_one_and_update(
            {"_id" : id_empacotadeira},
            {'$set':
                {'hora_fim_alarme':hora_alarme_fim.strftime('%H:%M')
                }           
            },upsert=True
        )
    except:
        print("Erro ao salvar dados de quando desligou empacotadeira")



def ligou_empac_status():
    try:
        global id_empacotadeira_ativa
        colecao_torras = cliente.sowelo.empacotadeiras   
        doc = colecao_torras.find_one({"_id" : id_empacotadeira_ativa})     
        colecao_torras.find_one_and_update(
            {"_id" : id_empacotadeira_ativa},
            {'$set':
                {'status': True 
                }           
            },upsert=True
        )
    except:
        print("Erro ao salvar status da empacotadeira")

def desligou_empac_status():
    try:
        global id_empacotadeira_ativa
        colecao_torras = cliente.sowelo.empacotadeiras   
        doc = colecao_torras.find_one({"_id" : id_empacotadeira_ativa})     
        colecao_torras.find_one_and_update(
            {"_id" : id_empacotadeira_ativa},
            {'$set':
                {'status': False 
                }           
            },upsert=True
        )
    except:
        print("Erro ao salvar status da empacotadeira")

def ativou_alarme_status():
    try:
        global id_empacotadeira_ativa
        colecao_torras = cliente.sowelo.empacotadeiras   
        doc = colecao_torras.find_one({"_id" : id_empacotadeira_ativa})     
        colecao_torras.find_one_and_update(
            {"_id" : id_empacotadeira_ativa},
            {'$set':
                {'alarme': True 
                }           
            },upsert=True
        )
    except:
        print("Erro ao salvar status da Alarme")

def desativou_alarme_status():
    try:
        global id_empacotadeira_ativa
        colecao_torras = cliente.sowelo.empacotadeiras   
        doc = colecao_torras.find_one({"_id" : id_empacotadeira_ativa})     
        colecao_torras.find_one_and_update(
            {"_id" : id_empacotadeira_ativa},
            {'$set':
                {'alarme': False 
                }           
            },upsert=True
        )
    except:
        print("Erro ao salvar status do alarme")

def atualiza_pacotes_dia():
    try:
        global id_pacotes_dia
        global peso
        global contador_embalagens
        global embalagem
        busca_embalagem()
        colecao_pacotesdia = cliente.sowelo.pacotesdias   
        doc = colecao_pacotesdia.find_one({"_id" : id_pacotes_dia})
        data_hoje = datetime.now()
        data_comparar = data_hoje.strftime('%Y-%m-%d')   
        qtd_atual =   doc['qtd_embaladas']
        data_atual = doc['data_atual']
        peso_atual = doc['peso'] 
        if(data_comparar == data_atual):
            print("datas iguais")    
            colecao_pacotesdia.find_one_and_update(
                {"_id" : id_pacotes_dia},
                {'$set':
                    {'qtd_embaladas':  qtd_atual + contador_embalagens,
                     'peso' : peso_atual + (contador_embalagens * peso),
                     'embalagem' : embalagem                     
                     }                        
                },upsert=True
        )
    except:
        print("Erro atualizar pacotes dia")


def atualiza_sistema_moagem():
        try:
            print("Entrou pra atualizar sistema de moagem")
            global peso
            global contador_embalagens
            global id_sistema_moagem
            
            # # ler o que está no plc
            # ler_silos = tcp.read_holding_registers(slave_id=1, starting_address=1,quantity=20)
            # valores_lidos_silos = tcp.send_message(ler_silos, sock)
            sismoagem = cliente.sowelo.sistemamoagems
            #armazena na varieval doc blend a colecao
            docmoagens = sismoagem.find_one({"_id" : id_sistema_moagem})   # id silo 1
            #armazena na variavel qtd_silo_atual a qtd atual do silo que leu
            qtd_atual =   docmoagens['qtd_atual']
            # print("qtdatualsilo" , qtd_silo_atual)
            sismoagem.find_one_and_update(
                {"_id" : (id_sistema_moagem)},
                {'$set':
                    {'qtd_atual':qtd_atual - (contador_embalagens * peso)}
                },upsert=True
            )
        except erro:
            print("Erro ao Atualizar Sistema de Moagem", erro)


def atualiza_pacotes_automatico():
    try:
        global id_pacotes_dia
        colecao_pacotesdia = cliente.sowelo.pacotesdias  
        doc = colecao_pacotesdia.find_one({"_id" : id_pacotes_dia})
        data_hoje = datetime.now()        
        data_comparar = data_hoje.strftime('%Y-%m-%d')   
        qtd_atual =   doc['qtd_embaladas']
        data_atual = doc['data_atual']  
        if(data_comparar == data_atual):
            print("datas iguais")    
            colecao_pacotesdia.find_one_and_update(
                {"_id" : id_pacotes_dia},
                {'$set':
                    {'qtd_embaladas':  qtd_atual + contador_embalagens }                        
                },upsert=True
        )
    except:
        print("Erro ao atualizar pacotes automatico")

def atualizar_valores():
    try:
        global id_pacotes_dia
        print(id_pacotes_dia)
        colecao_pacotesdia = cliente.sowelo.pacotesdias    
        doc = colecao_pacotesdia.find_one({"_id":id_pacotes_dia})
        print(doc)
        data_hoje = datetime.now()    
        data_comparar = data_hoje.strftime('%Y-%m-%d')   
        qtd_atual =  doc['qtd_embaladas']
        data_atual = doc['data_atual']  
        if(data_comparar != data_atual):
            print("data diferentes")    
            colecao_pacotesdia.find_one_and_update(
                {"_id" : id_pacotes_dia},
                {'$set':
                    {'qtd_embaladas':  0,
                    'peso':  0,
                    "data_atual" : data_hoje.strftime('%Y-%m-%d')} 
                },upsert=True
            )
    except:
        print("Erro ao atualizar valores pacotes por dia")


while condicao:

    if(clp_conectado == True):                  
        rotinaprincipal()
        contador+=1
        contador_geral+=1
    elif(clp_conectado == False):
        print('CLP NÂO CONECTADO')          
        conecta_clp()
    if(desliga == 1):
        print('CLP NÂO CONECTADO')
        condicao=False             
        sock.close()
