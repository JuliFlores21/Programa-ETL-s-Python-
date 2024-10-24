import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_dates():
    try:
        # Conexión a la base de datos de staging
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'Anto2003#'
        db = 'staging'

        con_sta_db = Db_Connection(type, host, port, user, pwd, db)
        ses_sta_db = con_sta_db.start()

        # Extraemos los datos transformados de tra_dates
        sql_stmt = "SELECT date_bk,date_month,date_year FROM tra_date"
        dates_tra = pd.read_sql(sql_stmt, ses_sta_db)

        # Conexión a la base de datos de sor (data warehouse)
        db = 'sor'

        con_sor_db = Db_Connection(type, host, port, user, pwd, db)
        ses_sor_db = con_sor_db.start()
        dim_date_dict= {
            "date_bk":[],
            "date_month":[],
            "date_year":[],
        }
        if not dates_tra.empty:
            for bk,datmon,datyea \
             in zip(dates_tra['date_bk'],dates_tra['date_month'],dates_tra['date_year']):
                dim_date_dict['date_bk'].append(bk)
                dim_date_dict['date_month'].append(datmon)
                dim_date_dict['date_year'].append(datyea)


        if dim_date_dict['date_bk']:
            df_dim_date= pd.DataFrame(dim_date_dict)
            df_dim_date.to_sql("dim_date",ses_sor_db,if_exists="append",index=False)
        print("Cargando datos de store en sor")

    except:
        traceback.print_exc()

    finally:
        pass