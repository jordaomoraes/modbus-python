# Liga o Torrador e Grava data e hora
# adiciona a torra quando torrando está 1
# adiciona tempo de chamine, quando variavel está 1
# quando joga a agua, insere o set point, ou insere quando a variavel torrando volta pra zero
# ao acionar o elevador torrado, le a variavel que está o peso que foi torrado e salva na tabela.

import socket
import time
from umodbus import conf
from umodbus.client import tcp
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import datetime as dt
from bson.objectid import ObjectId
import variaveistorrador
URL = "mongodb+srv://actual:actual@jera-opx2u.mongodb.net/sowelo?retryWrites=true&w=majority"
# URL = "mongodb://jera02:actual4941@mongodb.jera.agr.br/jera02"
# URL = "mongodb://localhost/sowelo"

# variaveis para condições de loop, ativo ou não ativo
condicao = True
ligado = False
torra_em_andamento = False
setpoint_inserido = False
chamine_aberta = False
agua_aberta = False
mexedor_ligado = False
elevador_torrado_ativo = False

porta_ar_frio_aberta = False
porta_cilindro_aberta = False
pistao_cafe_cru_aberto = False



response = []
contador = 0
desliga = 3
contador_torrando = 0
grao_curva = 0
ar_curva = 0
operador = 0
lista_tempo_torra = []
lista_temperatura_torra = []

# variaveis para configurar os endereços do CLP
# # COILS - 0 ou 1
# clp_torrador_onoff = 41000
# clp_torrando = 41001
# clp_chamine = 41002
# clp_agua = 41003
# clp_elevador_torrado = 41004
# clp_mexedor = 41005
# clp_porta_cilindro = 41006
# clp_porta_cafe_cru = 41007
# clp_porta_ar_frio = 41008

clp_torrador_onoff = 0
clp_torrando = 1
clp_chamine = 2
clp_agua = 3
clp_elevador_torrado = 4
clp_mexedor = 5
clp_porta_cilindro = 6
clp_porta_cafe_cru = 7
clp_porta_ar_frio = 8

# variaveis para configurar os endereços do CLP
# # HOlDERS REGISTERS - 0 ou 32000
# clp_temp_grao = 8000
# clp_temp_ar = 8001
# clp_temp_forno = 8002
# clp_tempo_torra = 8004
# clp_peso_torrado = 8003

clp_temp_grao = 10
clp_temp_ar = 11
clp_temp_forno = 12
clp_tempo_torra = 13
clp_peso_torrado = 14
clp_operador = 20
clp_conectado = False
sock = 1


usuario_ativo = ObjectId("5e3305df44faf350b03c87b0")
funcionario = ""
id_sistema_moagem = ObjectId("5e3306f644faf350b03c87b5")
#usado para a manutenção e status do torrador
id_torrador_ativo = ObjectId("5e33068a44faf350b03c87b4")
cliente = MongoClient(URL)
data_e_hora_atuais = datetime.now()
# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# sock.connect(('localhost', 502))

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
            #sock.connect(('192.168.0.10', 502))
        except:
            print("Tentando Conectar com o CLP")
    if(clp_conectado == True):
        print("Maquina Conectada - Pronta pra Uso")



def rotinaprincipal():
    try:
        global ligado, torra_em_andamento, chamine_aberta, agua_aberta, elevador_torrado_ativo, mexedor_ligado
        global porta_ar_frio_aberta, porta_cilindro_aberta, clp_conectado
        global pistao_cafe_cru_aberto
        global response, contador, contador_torrando,clp_operador, operador
        global setpoint_inserido, grao_curva
        global lista_tempo_torra, lista_temperatura_torra
        global id_torrador_ativo
        global sock

        # message = tcp.read_holding_registers(slave_id=1, starting_address=1,quantity=10)
        message = tcp.read_coils(slave_id=1, starting_address=clp_torrador_onoff,quantity=1)
        response = tcp.send_message(message, sock)
        ligou = response[0]

        leitura_torrando = tcp.read_coils(slave_id=1, starting_address=clp_torrando,quantity=1)
        res_torrando = tcp.send_message(leitura_torrando, sock)
        torrando = res_torrando[0]

        leitura_chamine = tcp.read_coils(slave_id=1, starting_address=clp_chamine,quantity=1)
        res_chamine = tcp.send_message(leitura_chamine, sock)
        chamine = res_chamine[0]

        leitura_agua = tcp.read_coils(slave_id=1, starting_address=clp_agua,quantity=1)
        res_agua = tcp.send_message(leitura_agua, sock)
        agua = res_agua[0]

        leitura_elevador_torrado = tcp.read_coils(slave_id=1, starting_address=clp_elevador_torrado,quantity=1)
        res_elevador_torrado = tcp.send_message(leitura_elevador_torrado, sock)
        elevador_torrado = res_elevador_torrado[0]

        leitura_mexedor = tcp.read_coils(slave_id=1, starting_address=clp_mexedor,quantity=1)
        res_mexedor = tcp.send_message(leitura_mexedor, sock)
        mexedor = res_mexedor[0]

        leitura_porta_ar_frio = tcp.read_coils(slave_id=1, starting_address=clp_porta_ar_frio,quantity=1)
        res_porta_ar_frio = tcp.send_message(leitura_porta_ar_frio, sock)
        porta_ar_frio = res_porta_ar_frio[0]
                # print(porta_ar_frio)

        leitura_porta_cafe_cru = tcp.read_coils(slave_id=1, starting_address=clp_porta_cafe_cru,quantity=1)
        res_porta_cafe_cru = tcp.send_message(leitura_porta_cafe_cru, sock)
        porta_cafe_cru = res_porta_cafe_cru[0]

        leitura_porta_cilindro = tcp.read_coils(slave_id=1, starting_address=clp_porta_cilindro,quantity=1)
        res_porta_cilindro = tcp.send_message(leitura_porta_cilindro, sock)
        porta_cilindro = res_porta_cilindro[0]

        leitura_grao_curva = tcp.read_holding_registers(slave_id=1, starting_address=clp_temp_grao,quantity=1)
        res_grao_curva = tcp.send_message(leitura_grao_curva, sock)
        grao_curva = res_grao_curva[0]

        leitura_ar_curva = tcp.read_holding_registers(slave_id=1, starting_address=clp_temp_ar,quantity=1)
        res_ar_curva = tcp.send_message(leitura_ar_curva, sock)
        ar_curva = res_ar_curva[0]


        leitura_operador = tcp.read_holding_registers(slave_id=1, starting_address=clp_operador,quantity=1)
        res_operador = tcp.send_message(leitura_operador, sock)
        operador = res_operador[0]

        print('Lendo Informações CLP')

        if(ligado == False):
            fechou_agua_status()
            nao_torrando_status()
            desligou_mexedor_status()   
            fechou_chamine_status()
            torrador_desligado()
            fechou_porta_cilindro_status()

        if(ligado == True):
            try:  
                #print("Atualizou temperaturas", ar_curva)
                colecao_torras = cliente.sowelo.torradores
                doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
                colecao_torras.find_one_and_update(
        {"_id" : id_torrador_ativo},
        {'$set':
            {'temperatura_grao': grao_curva,
            'temperatura_ar': ar_curva
            }
        },upsert=True
                )
            except:
                print("Erro ao atualizar dados")
        # print("Torrador Ligado", ar_curva)
            #atualiza_temp_grao_ar()

        if (ligou == 1 and ligado == False ):
            try:
                ligou_torrador()
                torrador_ligado() 
                print('ligou Torrador')
                ligado = True
                contador = 0
                fechou_agua_status()
                nao_torrando_status()
                desligou_mexedor_status()   
                fechou_chamine_status()
            except:
                print("Erro ao inserir dados ON OFF torrador")

        elif(ligou == 0 and ligado == True):        
            print('Desligou Torrador' , contador * 2)
            desligou_torrador()
            torrador_desligado()
            ligado = False

        if(porta_ar_frio == 1 and porta_ar_frio_aberta == False):
            print("Abriu porta Ar frio")
            porta_ar_frio_aberta = True
            abriu_porta_ar_frio_status()          
        elif(porta_ar_frio ==0 and  porta_ar_frio_aberta == True):
            print("Fechou porta Ar frio")
            porta_ar_frio_aberta = False
            fechou_porta_ar_frio_status()


        if (torrando == 1 and torra_em_andamento == False ):
            try:
                pega_ultimo_id_blend_cru()
                pega_peso_total_ultimo_blend()
                iniciou_torra()
                setpoint_inserido = False
                print('Iniciou Torra')
                torrando_status()
                contador_torrando = 0
                torra_em_andamento = True
            except:
                print("Erro ao inserir dados da torra")
        elif(torrando == 0 and torra_em_andamento == True) :
            print('Finalizou a torra')
            # print("lista Tempo", lista_tempo_torra)
            # print("lista Temperatura", lista_temperatura_torra)
            torra_em_andamento = False
            fim_torra()
            nao_torrando_status()
            contador_torrando = 0
            # curva_torra()
            lista_temperatura_torra.clear
            lista_tempo_torra.clear
        if (torrando == 1 and torra_em_andamento == True):
            try:
                # print("Torrando - ", contador_torrando)
                # print("Grão - ", grao_curva)
                lista_tempo_torra.append(contador_torrando)
                lista_temperatura_torra.append(grao_curva)  
            except:
                print("Erro ao inserir dados da torra")    
        if(chamine == 1 and chamine_aberta == False):
            try:
                print("abriu chamine")
                abriu_chamine()
                chamine_aberta = True
                abriu_chamine_status()
            except:
                print("Erro ao inserir dados da Chaminé")
        elif(chamine == 0 and chamine_aberta == True):
            print("fechou chamine")
            chamine_aberta = False
            fechou_chamine_status()
        if(agua ==1 and agua_aberta == False):
            try:
                print("abriu agua")
                abriu_agua()
                agua_aberta = True
                setpoint_inserido = True
                insere_set_point()
                abriu_agua_status()
            except:
                print("Erro ao inserir dados da agua")
        elif(agua ==0 and agua_aberta == True):
            print("fechou agua")
            agua_aberta = False
            fechou_agua_status()

        if(elevador_torrado ==1 and elevador_torrado_ativo == False):
            try:
                print("Ativou elevador_torrado")
                elevador_torrado_ativo = True
                insere_peso_cafe_torrado()
                abriu_elevador_torrado_status()
                atualiza_sistema_moagem()
                print("Inseriu peso cafe torrado")
            except:
                print("Não Ativo, nenhuma torra ainda feita")

        elif(elevador_torrado ==0 and elevador_torrado_ativo == True):
            print("fechou elevador_torrado")
            elevador_torrado_ativo = False
            fechou_elevador_torrado_status()
            
        if(mexedor ==1 and mexedor_ligado == False):
            print("Ligou Mexedor")
            mexedor_ligado = True
            ligou_mexedor_status()
        elif(mexedor ==0 and mexedor_ligado == True):
            print("Desligou Mexedor")
            mexedor_ligado = False
            desligou_mexedor_status()      

        if(porta_cilindro == 1 and porta_cilindro_aberta == False):
            print("Abriu porta cilindro")
            porta_cilindro_aberta = True
            abriu_porta_cilindro_status()          
        elif(porta_cilindro ==0 and porta_cilindro_aberta == True):
            print("Fechou porta cilindro")
            porta_cilindro_aberta = False
            fechou_porta_cilindro_status()
        if(porta_cafe_cru == 1 and pistao_cafe_cru_aberto == False):
            print("Abriu pistao_cafe_cru")
            pistao_cafe_cru_aberto = True
            abriu_pistao_cafe_cru_status()          
        elif(porta_cafe_cru ==0 and pistao_cafe_cru_aberto == True):
            print("Fechou pistao_cafe_cru")
            pistao_cafe_cru_aberto = False
            fechou_pistao_cafe_cru_status()   

        time.sleep(2)
    except:
        print("erro ao ler variaveis do clp!")
        clp_conectado = False

def iniciou_torra():
        global id_torra_ativa, id_ultimo_blend, id_torrador_ativo
        global operador
        busca_nome_funcionario()
        
        colecao_peso_ultimo_blend = cliente.sowelo.pro_blendcrus
        docpesoblend = colecao_peso_ultimo_blend.find_one({"_id" : id_ultimo_blend})
        peso_total_blend =   docpesoblend['peso_total']
        global funcionario
        global data_inicio_torra
        data_inicio_torra = dt.datetime.now()
        post = { "data_torra" : data_inicio_torra.strftime('%Y-%m-%d'),
                "hora_torra" : data_inicio_torra.strftime('%H:%M'),
                "usuario" : usuario_ativo,
                "torrador" : id_torrador_ativo,
                "funcionario" : funcionario,
                "cod_blendcru" : ObjectId(id_ultimo_blend),
                "peso_cafe_cru" : peso_total_blend
                }
        posts_torra_ativa = cliente.sowelo.torras
        id_torra_ativa = posts_torra_ativa.insert_one(post).inserted_id
        print("torra ativa " , id_torra_ativa)

def busca_nome_funcionario():
    global operador
    global funcionario
    colecao_funcionarios = cliente.sowelo.funcionarios
    docfuncionarios = colecao_funcionarios.find_one({"cod_funcionario" : operador})
    funcionario = docfuncionarios['_id']
   


def curva_torra():
        global id_torra_ativa
        global contador_torrando, grao_curva
        global lista_temperatura_torra, lista_tempo_torra
        print("Inserindo no banco  a Curva torra")      
        post = { "id_torra" : id_torra_ativa,            
                "tempo" : lista_tempo_torra,
                "temperatura" : lista_temperatura_torra,
                "usuario" : usuario_ativo
                }
        posts_curva = cliente.sowelo.curvatorra
        id_curva = posts_curva.insert_one(post).inserted_id
        print("torra ativa " , id_curva)

def fim_torra():
        try:
            global id_torra_ativa, id_torra_resfriamento
            global data_fim_torra
            global setpoint_inserido
            global lista_temperatura_torra, lista_tempo_torra
            ler_tempo_da_torra()

            if(setpoint_inserido == False):
                insere_set_point()
            data_fim_torra = dt.datetime.now()
            colecao_fim_torras = cliente.sowelo.torras
            doc = colecao_fim_torras.find_one({"_id" : id_torra_ativa})
            colecao_fim_torras.find_one_and_update(
                {"_id" : id_torra_ativa},
                {'$set':
                    {'data_fim_torra': data_fim_torra.strftime('%Y-%m-%d'),
                    'hora_fim_torra': data_fim_torra.strftime('%H:%M'),
                    'duracao_total_torra':tempo_de_torra,
                    'curva_torra.tempo':lista_tempo_torra,
                    'curva_torra.temperatura':lista_temperatura_torra
                    }
                },upsert=True
            )
            id_torra_resfriamento = id_torra_ativa
            print("torra resfriando " , id_torra_resfriamento)
        except:
            print("Erro ao inserir dados do Fim da torra")

def atualiza_sistema_moagem():
        try:
            global peso_cafe_torrado
            global id_sistema_moagem
            ler_peso_cafe_torrado()
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
                    {'qtd_atual':qtd_atual + peso_cafe_torrado}
                },upsert=True
            )
        except:
            print("Erro ao Atualizar Sistema de Moagem")

def insere_set_point():
        try:
            global temperatura_atual_grao
            global id_torra_ativa

            ler_temperaturas()
            colecao_setpoint = cliente.sowelo.torras
            docsetpoint = colecao_setpoint.find_one({"_id" : id_torra_ativa})
            colecao_setpoint.find_one_and_update(
                {"_id" : id_torra_ativa},
                {'$set':
                    {'setpoint': temperatura_atual_grao}
                },upsert=True
            )
            print("inseriu set point")
        except:
            print("Erro ao Inserir o Set Point")


def insere_peso_cafe_torrado():
        try:
            global temperatura_atual_grao
            global id_torra_resfriamento
            ler_peso_cafe_torrado()
            colecao_cafe_torrado = cliente.sowelo.torras
            doccafe_torrado = colecao_cafe_torrado.find_one({"_id" : id_torra_resfriamento})
            colecao_cafe_torrado.find_one_and_update(
                {"_id" : id_torra_resfriamento},
                {'$set':
                    {'peso_cafe_torrado': peso_cafe_torrado}
                },upsert=True
            )
        except:
            print("Erro ao inserir peso Cafe Torrado")

def ler_temperaturas():
        try:
            global temperatura_atual_grao, temperatura_atual_ar,temperatura_atual_forno, tempo_de_torra
            leitura_temperatura_atual_grao = tcp.read_holding_registers(slave_id=1, starting_address=clp_temp_grao,quantity=1)
            res_temperatura_atual_grao = tcp.send_message(leitura_temperatura_atual_grao, sock)
            temperatura_atual_grao = res_temperatura_atual_grao[0]
            leitura_temperatura_atual_ar = tcp.read_holding_registers(slave_id=1, starting_address=clp_temp_ar,quantity=1)
            res_temperatura_atual_ar = tcp.send_message(leitura_temperatura_atual_ar, sock)
            temperatura_atual_ar = res_temperatura_atual_ar[0]
            leitura_temperatura_atual_forno = tcp.read_holding_registers(slave_id=1, starting_address=clp_temp_forno,quantity=1)
            res_temperatura_atual_forno = tcp.send_message(leitura_temperatura_atual_forno, sock)
            temperatura_atual_forno = res_temperatura_atual_forno[0]
            leitura_tempo_de_torra = tcp.read_holding_registers(slave_id=1, starting_address=clp_tempo_torra,quantity=1)
            res_tempo_de_torra = tcp.send_message(leitura_tempo_de_torra, sock)
            tempo_de_torra = res_tempo_de_torra[0]
            print(temperatura_atual_grao , temperatura_atual_ar , temperatura_atual_forno)
        except:
            print("Erro ao Ler Temperaturas")

def ler_tempo_da_torra():
        try:
            global tempo_de_torra
            leitura_tempo_de_torra = tcp.read_holding_registers(slave_id=1, starting_address=clp_tempo_torra,quantity=1)
            res_tempo_de_torra = tcp.send_message(leitura_tempo_de_torra, sock)
            tempo_de_torra = res_tempo_de_torra[0]
            print("Tempo total da torra " , tempo_de_torra)
        except:
            print("Erro ao Ler tempo a torra")

def ler_peso_cafe_torrado():
        try:
            global peso_cafe_torrado
            leitura_peso_cafe_torrado = tcp.read_holding_registers(slave_id=1, starting_address=clp_peso_torrado,quantity=1)
            res_peso_cafe_torrado = tcp.send_message(leitura_peso_cafe_torrado, sock)
            peso_cafe_torrado = res_peso_cafe_torrado[0]
        except:
            print("Erro ao Ler peso Cafe torrado")

def abriu_chamine():
        try:
            global temperatura_atual_grao,  tempo_de_torra, id_torra_ativa
            ler_temperaturas()
            colecao_torras = cliente.sowelo.torras
            doc = colecao_torras.find_one({"_id" : id_torra_ativa})
            colecao_torras.find_one_and_update(
                {"_id" : id_torra_ativa},
                {'$set':
                    {'abertura_chamine.temp_grao': temperatura_atual_grao,
                    'abertura_chamine.tempo_de_torra': tempo_de_torra
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir dados da inserir a chamine")

def atualiza_temp_grao_ar():
        try:
            global id_torrador_ativo
            global grao_curva 
            global ar_curva
            print("Atualizou temperaturas", ar_curva)
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'temperatura_grao': grao_curva,
                    'temperatura_ar': ar_curva
                    }
                },upsert=True
            )
        except:
            print("Erro ao atualizar dados")

def abriu_chamine_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'chamine': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da chamine")

def fechou_chamine_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'chamine': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da chamine")


def abriu_pistao_cafe_cru_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'pistao_cafe_cru': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da pistao_cafe_cru")

def fechou_pistao_cafe_cru_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'pistao_cafe_cru': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da pistao_cafe_cru")



def abriu_elevador_torrado_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'elevador_torrado': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da elevador_torrado")

def fechou_elevador_torrado_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'elevador_torrado': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da elevador_torrado")


def abriu_porta_ar_frio_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'porta_ar_frio': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da porta_ar_frio")

def fechou_porta_ar_frio_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'porta_ar_frio': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status da porta_ar_frio")


def torrador_ligado():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'torrador': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status do Torrador")

def torrador_desligado():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'torrador': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status do Torrador")

def abriu_agua():
        try:
            leitura_tempo_agua = tcp.read_holding_registers(slave_id=1, starting_address=clp_tempo_torra,quantity=1)
            res_tempo_agua = tcp.send_message(leitura_tempo_agua, sock)
            tempo_agua = res_tempo_agua[0]
            global temperatura_atual_grao, tempo_de_torra, id_torra_ativa
            ler_temperaturas()
            colecao_torras = cliente.sowelo.torras
            doc = colecao_torras.find_one({"_id" : id_torra_ativa})
            colecao_torras.find_one_and_update(
                {"_id" : id_torra_ativa},
                {'$set':
                    {'abertura_agua.temp_grao': temperatura_atual_grao,
                    'abertura_agua.tempo_de_torra': tempo_de_torra,
                    'abertura_agua.duracao': tempo_agua
                    }
                },upsert=True
            )
        except:
            print("erro ao inserir dados da agua")

def abriu_agua_status():
        try:
            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'agua': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir Status da Agua")

def fechou_agua_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'agua': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir Status da Agua")


def abriu_porta_cilindro_status():
        try:
            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'porta_cilindro': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir Status da porta_cilindro")

def fechou_porta_cilindro_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'porta_cilindro': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir Status da porta_cilindro")

def torrando_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'torrando': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir Status torrando")

def nao_torrando_status():
        try:
            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'torrando': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir de não torrando")

def ligou_mexedor_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'mexedor': True
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status do mexedor")

def desligou_mexedor_status():
        try:

            global id_torrador_ativo
            colecao_torras = cliente.sowelo.torradores
            doc = colecao_torras.find_one({"_id" : id_torrador_ativo})
            colecao_torras.find_one_and_update(
                {"_id" : id_torrador_ativo},
                {'$set':
                    {'mexedor': False
                    }
                },upsert=True
            )
        except:
            print("Erro ao inserir status do mexedor")



def pega_ultimo_id_blend_cru():
        try:
            global id_ultimo_blend
            colecao_ultimo_blend = cliente.sowelo.pro_blendcrus
            id_ultimo_blend = colecao_ultimo_blend.find().sort('_id', -1).limit(1)[0]['_id']
        except:
            print("Erro ao selecionar id ultimo Blend")

def pega_peso_total_ultimo_blend():
        try:
            global id_ultimo_blend
            colecao_peso_ultimo_blend = cliente.sowelo.pro_blendcrus
            docpesoblend = colecao_peso_ultimo_blend.find_one({"_id" : id_ultimo_blend})
            peso_total_blend =   docpesoblend['peso_total']
        except:
            print("Erro ao selecionar peso ultimo Blend")


def ligou_torrador():
        try:
            global data_ligou
            data_ligou = dt.datetime.now()
            global id_torrador
            #blend é torrador de blend
            post = { "data_ligou" : data_ligou.strftime('%Y-%m-%d'),
                    "hora_ligou" : data_ligou.strftime('%H:%M'),
                    "usuario" : usuario_ativo,
                    }
            posts = cliente.sowelo.onofftorrador
            id_torrador = posts.insert_one(post).inserted_id
        except:
            print("Erro ao inserir dados de ligou torrador")

def desligou_torrador():
        try:
            global id_torrador
            data_desligou = dt.datetime.now()
            global data_ligou
            global tempo_ligado
            global contador
        
            colecao = cliente.sowelo.onofftorrador
            doc = colecao.find_one({"_id" : id_torrador})
        
            colecao.find_one_and_update(
                {"_id" : id_torrador},
                {'$set':
                    {'data_desligou': data_desligou.strftime('%Y-%m-%d'),
                    'hora_desligou': data_desligou.strftime('%H:%M'),
                    'tempo_ligado': contador*2}
                },upsert=True
            )

            maquina_torrador = cliente.sowelo.torradores
            docmaquina = maquina_torrador.find_one({"_id" : id_torrador_ativo})    
            oleos_redutores_atual = (docmaquina['manutencao']['oleos_redutores']['temp_atual'])
            bicos_filtros_queimador_atual = (docmaquina['manutencao']['bicos_filtros_queimador']['temp_atual'])
            limpeza_tubulacoes_atual = (docmaquina['manutencao']['limpeza_tubulacoes']['temp_atual'])
            painel_eletrico_atual = (docmaquina['manutencao']['painel_eletrico']['temp_atual'])
            rolamentos_atual = (docmaquina['manutencao']['rolamentos']['temp_atual'])
            limpeza_ciclones_atual = (docmaquina['manutencao']['limpeza_ciclones']['temp_atual'])
            limpeza_baixo_mexedor_atual = (docmaquina['manutencao']['limpeza_baixo_mexedor']['temp_atual'])
            limpeza_cilindro_externo_atual = (docmaquina['manutencao']['limpeza_cilindro_externo']['temp_atual'])
            bicos_injetores_agua_atual = (docmaquina['manutencao']['bicos_injetores_agua']['temp_atual'])
            limpeza_caixa_pelicula_atual = (docmaquina['manutencao']['limpeza_caixa_pelicula']['temp_atual'])
            rolamento_ventilador_cima_atual = (docmaquina['manutencao']['rolamento_ventilador_cima']['temp_atual'])
            
            
            maquina_torrador.find_one_and_update(

                {"_id" : id_torrador_ativo},
                {'$set':
                    {'manutencao.oleos_redutores.temp_atual': oleos_redutores_atual + contador * 2,                          
                    'manutencao.bicos_filtros_queimador.temp_atual': bicos_filtros_queimador_atual + contador * 2,                          
                    'manutencao.limpeza_tubulacoes.temp_atual': limpeza_tubulacoes_atual + contador * 2,                          
                    'manutencao.painel_eletrico.temp_atual': painel_eletrico_atual + contador * 2,                          
                    'manutencao.rolamentos.temp_atual': rolamentos_atual + contador * 2,                          
                    'manutencao.limpeza_ciclones.temp_atual': limpeza_ciclones_atual + contador * 2,                          
                    'manutencao.limpeza_baixo_mexedor.temp_atual': limpeza_baixo_mexedor_atual + contador * 2,                          
                    'manutencao.limpeza_cilindro_externo.temp_atual': limpeza_cilindro_externo_atual + contador * 2,                          
                    'manutencao.bicos_injetores_agua.temp_atual': bicos_injetores_agua_atual + contador * 2,                          
                    'manutencao.limpeza_caixa_pelicula.temp_atual': limpeza_caixa_pelicula_atual + contador * 2,                          
                    'manutencao.rolamento_ventilador_cima.temp_atual': rolamento_ventilador_cima_atual + contador * 2                          
                    }           
                },upsert=True
            )
        except:
            print("Erro ao atualizar quando desliga o Torrador")

while condicao:        
    if(clp_conectado == True):                  
        rotinaprincipal()
        contador+=1
        contador_torrando +=2
    elif(clp_conectado == False):
        print('CLP NÂO CONECTADO')          
        conecta_clp() 

    if(condicao == False):
            print('Não Conectou ao CLP')
            #condicao=False
            sock.close()


