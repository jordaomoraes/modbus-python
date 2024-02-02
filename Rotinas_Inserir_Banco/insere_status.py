from datetime import datetime, timezone
from pymongo import MongoClient
import datetime as dt
from bson.objectid import ObjectId
# URL = "mongodb://jera02:actual4941@mongodb.jera.agr.br/jera02"
URL = "mongodb+srv://actual:actual@jera-opx2u.mongodb.net/test?retryWrites=true&w=majority"

# URL = "mongodb://localhost/sowelo"
usuario_ativo = ObjectId("5e3305df44faf350b03c87b0")
cliente = MongoClient(URL)
data_atual = dt.datetime.now()
    
    #blend é torrador de blend
post = { "status_blend" : 0,
            "usuario" : usuario_ativo            
            }
posts = cliente.sowelo.status
id_processo = posts.insert_one(post).inserted_id

 
print(id_processo)
