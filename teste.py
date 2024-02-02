from datetime import datetime, timezone
from pymongo import MongoClient
import datetime as dt
from bson.objectid import ObjectId
# URL = "mongodb://jera02:actual4941@mongodb.jera.agr.br/jera02"
URL = "mongodb+srv://actual:actual@jera-opx2u.mongodb.net/test?retryWrites=true&w=majority"

# URL = "mongodb://localhost/sowelo"
usuario_ativo = ObjectId("5e3305df44faf350b03c87b0")
id_pacotes_dia = ObjectId("5e347312063680e3e1e6bcac")
cliente = MongoClient(URL)
data_atual = dt.datetime.now()
    
    #blend é torrador de blend
post = { "data_atual" : data_atual.strftime('%Y-%m-%d'),
            "usuario" : usuario_ativo,
            "qtd_embaladas" : 0
            }
posts = cliente.sowelo.pacotesdia
id_processo = posts.insert_one(post).inserted_id

 
print(id_processo)


def atualizar_valores():
    global id_pacotes_dia
    print(id_pacotes_dia)
    colecao_pacotesdia = cliente.sowelo.pacotesdia   
 
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
                  "data_atual" : data_hoje.strftime('%Y-%m-%d')} 
            },upsert=True
        ) 

atualizar_valores()



