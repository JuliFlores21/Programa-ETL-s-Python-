import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_inventory():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'Anto2003#'
        db_staging = 'staging'
        db_sor = 'sor'
        
        # Conexión a la base de datos staging
        con_sta_db = Db_Connection(type, host, port, user, pwd, db_staging)
        ses_sta_db = con_sta_db.start()

        # Conexión a la base de datos SOR
        con_sor_db = Db_Connection(type, host, port, user, pwd, db_sor)
        ses_sor_db = con_sor_db.start()

        # Extraer datos de tra_inventory desde staging
        sql_stmt = """
            SELECT ti.id, ti.film_id, ti.store_id, ti.date_id, ti.rental_price, ti.rental_cost
            FROM staging.tra_inventory ti
        """
        inventory_tra = pd.read_sql(sql_stmt, ses_sta_db)

        dim_inven_dict= {
            "store_id":[],
            "film_id":[],
            "date_id":[],
            "rental_price":[],
            "rental_cost":[],
        }
        if not inventory_tra.empty:
            for sid,fid,did,rp,rc \
             in zip(inventory_tra['store_id'],inventory_tra['film_id'],inventory_tra['date_id'],inventory_tra['rental_price'],inventory_tra['rental_cost']):
                dim_inven_dict['store_id'].append(sid)
                dim_inven_dict['film_id'].append(fid)
                dim_inven_dict['date_id'].append(did)
                dim_inven_dict['rental_price'].append(rp)
                dim_inven_dict['rental_cost'].append(rc)


        if dim_inven_dict['store_id']:
            df_dim_inven= pd.DataFrame(dim_inven_dict)
            df_dim_inven.to_sql("fact_inventory",ses_sor_db,if_exists="append",index=False)
        print("Cargando datos de film en sor")

    except:
        traceback.print_exc()

    finally:
        pass