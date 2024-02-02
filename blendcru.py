#!/usr/bin/env python
# scripts/examples/simple_tcp_client.py
import socket
import time
from umodbus import conf
from umodbus.client import tcp
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import datetime as dt
from bson.objectid import ObjectId
import variaveis

# Enable values to be signed (default is False).
condicao = True
conf.SIGNED_VALUES = True
maquina_ligada = True
pesando = False
elevador_ativado = False
ligou = 0

# variaveis para configurar os endereços do CLP
# COILS - 0 ou 1
clp_maq_blend_onoff = 40000
clp_pesagem = 40002
clp_pediu_cafe = 40001
clp_desliga_loop = 40010


# variaveis para configurar os endereços do CLP
# HOlDERS REGISTERS - 0 ou 32000
clp_inicio_silos = 9000



#endereços ID dos silos cadastrados

# silo1 = ObjectId("5e3305fb44faf350b03c87b1")
# silo2 = ObjectId("5e33061044faf350b03c87b2")
# silo3 = ObjectId("5e33062344faf350b03c87b3")
# silo4 = ObjectId("5e34015284c8c75a783cec5a")

silo1 = 1
silo2 = 2
silo3 = 3

usuario = ObjectId("5e3305df44faf350b03c87b0") 
id_maquina_blend = ObjectId("5e33084b44faf350b03c87b7")
status = ObjectId("5e284dc8f414de30a45a90e2")

contador=0
contador_status = 0
contador_pesagem = 0
desliga = 3
ligado = False

clp_conectado = False
sock = 1

res_desliga_loop = []
# URL = "mongodb://jera02:actual4941@mongodb.jera.agr.br/jera02"
URL = "mongodb+srv://actual:actual@jera-opx2u.mongodb.net/sowelo?retryWrites=true&w=majority"
# URL = "mongodb://localhost/sowelo"
cliente = MongoClient(URL)
data_e_hora_atuais = datetime.now()
# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# sock.connect(('localhost', 502))
# sock.connect(('192.168.0.10', 502))
# Modbus TCP/IP.
# sock.close()


def conecta_clp():
    global clp_conectado
    global sock
    if(clp_conectado == False):
        try:    
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('localhost', 502))
            # sock.connect(('192.168.0.5', 502))
            clp_conectado = True
            print("Conexão Estabelecida")
            time.sleep(2)            
        except:
            print("Tentando Conectar com o CLP")
    if(clp_conectado == True):
        print("Maquina Conectada - Pronta pra Uso")


def ligou_maquina():
    try:
        global data_ligou
        data_ligou = dt.datetime.now()
        global id_maquina
        #blend é maquina de blend
        post = {"blend" : id_maquina_blend,
                "data_ligou" : data_ligou.strftime('%Y-%m-%d'),
                "hora_ligou" : data_ligou.strftime('%H:%M'),
                "usuario" : usuario,
                }
        posts = cliente.sowelo.onoffmaquinablends
        id_maquina = posts.insert_one(post).inserted_id
    except:
        print("Erro ao inserir dados de ligou a maquina")
    
  
def atualiza_status():
    try:
        #funcao que vai atualizar para ver se o sistema esta online
        colecao_status = cliente.sowelo.status
        doc = colecao_status.find_one({"_id" : status })     
        colecao_status.find_one_and_update(
            {"_id" : status},
            {'$set':
                {'status_blend': 0}           
            },upsert=True
        ) 
    except:
        print("Erro ao atualizar Status")


def desligou_maquina():
    try:
        global id_maquina, maquina_blend
        data_desligou = dt.datetime.now()  
        global data_ligou
        global tempo_ligado
        # print("id desliga" , id_maquina)
        colecao = cliente.sowelo.onoffmaquinablends   
        doc = colecao.find_one({"_id" : id_maquina})     
        tempo_ligado = data_desligou - data_ligou
        # print(tempo_ligado)   
        colecao.find_one_and_update(
            {"_id" : id_maquina},
            {'$set':
                {'data_desligou': data_desligou.strftime('%Y-%m-%d'),
                'hora_desligou': data_desligou.strftime('%H:%M'),
                'tempo_ligado': contador*2}           
            },upsert=True
        )  

        maquina_blend = cliente.sowelo.blendcrus   
        docmaquina = maquina_blend.find_one({"_id" : id_maquina_blend})    
        correia_canecas_atual = (docmaquina['manutencao']['correia_canecas']['temp_atual'])
        canecas_atual = (docmaquina['manutencao']['canecas']['temp_atual'])
        rolamento_pe_cabeca = (docmaquina['manutencao']['rolamento_pe_cabeca']['temp_atual'])
        oleo_redutor_transmissao = (docmaquina['manutencao']['oleo_redutor_transmissao']['temp_atual'])
        estica_correia_canecas = (docmaquina['manutencao']['estica_correia_canecas']['temp_atual'])
        aperto_porcas_canecas = (docmaquina['manutencao']['aperto_porcas_canecas']['temp_atual'])
        rolamento_rosca_abastece_cima_silo = (docmaquina['manutencao']['rolamento_rosca_abastece_cima_silo']['temp_atual'])
        rolamento_rosca_saida_balanca = (docmaquina['manutencao']['rolamento_rosca_saida_balanca']['temp_atual'])

        maquina_blend.find_one_and_update(
            {"_id" : id_maquina_blend},
            {'$set':
                {'manutencao.correia_canecas.temp_atual': correia_canecas_atual + contador * 2,
                'manutencao.canecas.temp_atual':canecas_atual + contador * 2,
                'manutencao.rolamento_pe_cabeca.temp_atual': rolamento_pe_cabeca + contador * 2,
                'manutencao.oleo_redutor_transmissao.temp_atual': oleo_redutor_transmissao + contador * 2,
                'manutencao.estica_correia_canecas.temp_atual': estica_correia_canecas + contador * 2,
                'manutencao.aperto_porcas_canecas.temp_atual': aperto_porcas_canecas + contador * 2,
                'manutencao.rolamento_rosca_abastece_cima_silo.temp_atual':  rolamento_rosca_abastece_cima_silo + contador * 2,
                'manutencao.rolamento_rosca_saida_balanca.temp_atual': rolamento_rosca_saida_balanca + contador * 2,                        
                }           
            },upsert=True
        )
    except:
        print("Erro ao atualizar dados quando o blend desliga")

def pediu_cafe(): # aqui atualiza a data que foi enviado ao torrador
    try:
        global id_blend  
        data_pediu_cafe = dt.datetime.now()  
        print("id blend" , id_blend)
        colecao_blends = cliente.sowelo.pro_blendcrus   
        docblends = colecao_blends.find_one({"_id" : id_blend})    
        colecao_blends.find_one_and_update(
            {"_id" : id_blend},
            {'$set':
                {'data_torrador': data_pediu_cafe.strftime('%Y-%m-%d'),
                'hora_torrador': data_pediu_cafe.strftime('%H:%M'),
                "usuario" : usuario}           
            },upsert=True
        )
    except:
        print("Erro ao inserir dados de Pediu café")

def atualiza_silos():
    try:
        #busca o valor de cada valor na receita de cada silo
        global silo1, silo2, silo3,silo4
        # ler o que está no plc
        ler_silos = tcp.read_holding_registers(slave_id=1, starting_address=clp_inicio_silos,quantity=5)  
        valores_lidos_silos = tcp.send_message(ler_silos, sock) 
        silos = cliente.sowelo.silos  
        #armazena na varieval doc blend a colecao
        docblends = silos.find_one({"_id" : silo1})   # id silo 1 
        #armazena na variavel qtd_silo_atual a qtd atual do silo que leu
        qtd_silo_atual =   docblends['qtd_atual'] 
        # print("qtdatualsilo" , qtd_silo_atual)
        silos.find_one_and_update(
            {"_id" : (silo1)},
            {'$set':
                {'qtd_atual':qtd_silo_atual - valores_lidos_silos[0]}           
            },upsert=True
        )
        print("Estoque no silo 1 atualizado")

        docblends2 = silos.find_one({"_id" : silo2})   # id silo 2
        #armazena na variavel qtd_silo_atual a qtd atual do silo que leu
        qtd_silo_atual2 =   docblends2['qtd_atual'] 
        # print("qtdatualsilo" , qtd_silo_atual)
        silos.find_one_and_update(
            {"_id" : silo2},
            {'$set':
                {'qtd_atual':qtd_silo_atual2 - valores_lidos_silos[1]}           
            },upsert=True
        )
        print("Estoque no silo 2 atualizado")

        docblends3 = silos.find_one({"_id" : silo3})   # id silo 3 
        #armazena na variavel qtd_silo_atual a qtd atual do silo que leu
        qtd_silo_atual3 =   docblends3['qtd_atual'] 
        # print("qtdatualsilo" , qtd_silo_atual)
        silos.find_one_and_update(
            {"_id" : silo3},
            {'$set':
                {'qtd_atual':qtd_silo_atual3 - valores_lidos_silos[2]}           
            },upsert=True
        )
        print("Estoque no silo 3 atualizado")

        docblends4 = silos.find_one({"_id" : silo4})   # id silo 3 
        #armazena na variavel qtd_silo_atual a qtd atual do silo que leu
        qtd_silo_atual4 =   docblends4['qtd_atual'] 
        # print("qtdatualsilo" , qtd_silo_atual)
        silos.find_one_and_update(
            {"_id" : silo4},
            {'$set':
                {'qtd_atual':qtd_silo_atual4 - valores_lidos_silos[3]}           
            },upsert=True
        )
        print("Estoque no silo 4 atualizado")
    except:
        print("Erro ao atualizar quantidade nos silos")

def insereblendcru():
    try:
        global id_blend, contador_pesagem, id_maquina_blend

        message3 = tcp.read_holding_registers(slave_id=1, starting_address=9000,quantity=20)  
        response3 = tcp.send_message(message3, sock)
        time.sleep(0.3)
        post = {"qtdcafeforasilo" : 0,
                "usado" : 0,

                "silos" : [{"silo": silo1, "qtd_retirada": response3[0]},
                        {"silo": silo2, "qtd_retirada":  response3[1]},
                        {"silo": silo3, "qtd_retirada":  response3[2]},
                        {"silo": silo4, "qtd_retirada":  response3[3]}],          
                
                "peso_total" : ( response3[0] + response3[1] + response3[2] + response3[3]),           
                "usuario" : usuario,
                "nome_blend" : "Automatico",
                "cod_clp" : 5,
                "maquina_blend" : id_maquina_blend,
                #"cafeforasilo" : ObjectId("5e1e044348ce451d445bdd2c"),
                "data_blend" : data_e_hora_atuais.strftime('%Y-%m-%d'),
                "hora_blend" : data_e_hora_atuais.strftime('%H:%M'),
                "duracao" : contador_pesagem *2,            
            }

        posts = cliente.sowelo.pro_blendcrus    
        id_blend = posts.insert_one(post).inserted_id
    except:
        print("Erro ao inserir dados mdo blend cru")
    


def loop_principal_blend():
    try:
        global ligado, pesando# é necessário declarar que vai usar a variavel aqui como global
        global response, contador, contador_status, contador_pesagem
        global clp_conectado
        global elevador_ativado
        global sock
        # message = tcp.read_holding_registers(slave_id=1, starting_address=1,quantity=10)  

        leitura_maq_blend_onoff = tcp.read_coils(slave_id=1, starting_address=clp_maq_blend_onoff,quantity=1)    
        res_maq_blend_onoff = tcp.send_message(leitura_maq_blend_onoff, sock)
        maq_blend_onoff = res_maq_blend_onoff[0]

        leitura_pesagem = tcp.read_coils(slave_id=1, starting_address=clp_pesagem,quantity=1)    
        res_pesagem = tcp.send_message(leitura_pesagem, sock)
        pesagem = res_pesagem[0]

        leitura_pediu_cafe = tcp.read_coils(slave_id=1, starting_address=clp_pediu_cafe,quantity=1)    
        res_pediu_cafe = tcp.send_message(leitura_pediu_cafe, sock)
        pediucafe = res_pediu_cafe[0]

        leitura_desliga_loop = tcp.read_coils(slave_id=1, starting_address=clp_desliga_loop,quantity=1)    
        res_desliga_loop = tcp.send_message(leitura_desliga_loop, sock)
        desliga_loop = res_desliga_loop[0]
              
        print('Lendo Informações CLP')

        if(ligado == False):
            desligou_maquina_blend_status()
            terminou_blendar_status()
            desligou_elevador_cru_status()

        if (maq_blend_onoff == 1 and ligado == False ):
            ligou_maquina()
            ligou_maquina_blend_status()
            print('ligou Maquina')
            ligado = True 
            contador = 0

        elif(maq_blend_onoff == 0 and ligado == True):       
            desligou_maquina()
            desligou_maquina_blend_status()
            print('Desligou Maquina' , contador * 2)
            ligado = False

        if(pediucafe == 1 and elevador_ativado == False):
            pediu_cafe()
            elevador_ativado = True
            print("ativou elevador")
            ligou_elevador_cru_status() 
            

        elif(pediucafe == 1 and elevador_ativado == True):
            print("Elevador cru ativo")

        if(pediucafe == 0 and elevador_ativado == True):
            print("Desativou elevador cru")
            desligou_elevador_cru_status()
            elevador_ativado = False            

        if(contador_status == 2):
            atualiza_status()
            contador_status = 0
        if(pesagem == 1 and pesando == False ):
            contador_pesagem = 0
            blendando_status()
            print("inicio pesagem - contador = ", contador_pesagem)
            pesando = True
        elif(pesagem == 0 and pesando == True):
            print("terminou a pesagem - contador - ", contador_pesagem *2)
            pesando = False
            terminou_blendar_status()
            insereblendcru()
            atualiza_silos()
            print("Atualizou Silo")

        time.sleep(2)
    except:
        print("Erro ao ler dados principais do clp")
        clp_conectado = False



def ligou_maquina_blend_status():
        try:
            print("Atualizou status")

            global id_maquina_blend
            colecao_maq_blend = cliente.sowelo.blendcrus
            doc = colecao_maq_blend.find_one({"_id" : id_maquina_blend})
            colecao_maq_blend.find_one_and_update(
                {"_id" : id_maquina_blend},
                {'$set':
                    {'ligado': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da  maquina ligado")


def desligou_maquina_blend_status():
        try:

            global id_maquina_blend
            colecao_maq_blend = cliente.sowelo.blendcrus
            doc = colecao_maq_blend.find_one({"_id" : id_maquina_blend})
            colecao_maq_blend.find_one_and_update(
                {"_id" : id_maquina_blend},
                {'$set':
                    {'ligado': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da  maquina desligado")



def ligou_elevador_cru_status():
        try:
            print("Atualizou status")

            global id_maquina_blend
            colecao_maq_blend = cliente.sowelo.blendcrus
            doc = colecao_maq_blend.find_one({"_id" : id_maquina_blend})
            colecao_maq_blend.find_one_and_update(
                {"_id" : id_maquina_blend},
                {'$set':
                    {'elevadorcru': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status do elevador cru")


def desligou_elevador_cru_status():
        try:

            global id_maquina_blend
            colecao_maq_blend = cliente.sowelo.blendcrus
            doc = colecao_maq_blend.find_one({"_id" : id_maquina_blend})
            colecao_maq_blend.find_one_and_update(
                {"_id" : id_maquina_blend},
                {'$set':
                    {'elevadorcru': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status do elevador cru")




def blendando_status():
        try:
            print("Atualizou status")

            global id_maquina_blend
            colecao_maq_blend = cliente.sowelo.blendcrus
            doc = colecao_maq_blend.find_one({"_id" : id_maquina_blend})
            colecao_maq_blend.find_one_and_update(
                {"_id" : id_maquina_blend},
                {'$set':
                    {'blendando': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da  maquina blendando")


def terminou_blendar_status():
        try:

            global id_maquina_blend
            colecao_maq_blend = cliente.sowelo.blendcrus
            doc = colecao_maq_blend.find_one({"_id" : id_maquina_blend})
            colecao_maq_blend.find_one_and_update(
                {"_id" : id_maquina_blend},
                {'$set':
                    {'blendando': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da  maquina finalizou blend")

while condicao:
    if(clp_conectado == True):                  
        loop_principal_blend()    
        contador+=1
        contador_status+=1
        contador_pesagem+=1
    elif(clp_conectado == False):
        print('CLP NÂO CONECTADO')          
        conecta_clp()   
    if(desliga == 1):
        print('Não Conectou ao CLP')
        #condicao=False             
        sock.close()


