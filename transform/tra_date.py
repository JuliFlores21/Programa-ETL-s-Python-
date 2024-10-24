from util.db_connection import Db_Connection
import traceback
import pandas as pd

def transformar_dates():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'Anto2003#'
        db = 'staging'
        con_db = Db_Connection(type,host,port,user,pwd,db)
        ses_db = con_db.start()
        if ses_db == -1:
           raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
           raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stmt = "SELECT date_id, date, month, year FROM ext_date"
        dates = pd.read_sql(sql_stmt, ses_db)

        # Convertimos la columna 'date' a tipo datetime si no lo está
        dates['date'] = pd.to_datetime(dates['date']).dt.date

        # Transformamos las fechas para que coincidan con last_update de films
        dates_transformed = dates[['date', 'month', 'year']].rename(columns={
            'date': 'date_bk',         # Usamos el valor de 'date' para 'date_bk'
            'month': 'date_month',
            'year': 'date_year'
        })
        return dates_transformed
        
    except:
        traceback.print_exc()
    finally:
        pass