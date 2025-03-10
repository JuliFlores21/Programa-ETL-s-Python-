from util.db_connection import Db_Connection
import traceback
import pandas as pd

def cargar_stores():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'Anto2003#'
        db = 'staging'
        con_sta_db = Db_Connection(type,host,port,user,pwd,db)
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
           raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
           raise Exception("Error tratando de conectarse a la base de datos ")
        sql_stmt =  "SELECT store_id, name, city, country From tra_store"
        
        stores_tra = pd.read_sql (sql_stmt, ses_sta_db)
        
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'Anto2003#'
        db = 'sor'
        con_sor_db = Db_Connection(type,host,port,user,pwd,db)
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
           raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
           raise Exception("Error tratando de conectarse a la base de datos ")
        
        dim_store_dict= {
            "store_bk":[],
            "name":[],
            "city":[],
            "country":[],
        }
        if not stores_tra.empty:
            for bk,nam,cit,cou \
             in zip(stores_tra['store_id'],stores_tra['name'],stores_tra['city'],stores_tra['country']):
                dim_store_dict['store_bk'].append(bk)
                dim_store_dict['name'].append(nam)
                dim_store_dict['city'].append(cit)
                dim_store_dict['country'].append(cou)

        if dim_store_dict['store_bk']:
            df_dim_store= pd.DataFrame(dim_store_dict)
            df_dim_store.to_sql("dim_store",ses_sor_db,if_exists="append",index=False)
        print("Cargando datos de store en sor")
    except:
        traceback.print_exc()
    finally:
        pass