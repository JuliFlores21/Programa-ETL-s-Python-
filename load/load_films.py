import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_films():
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
        sql_stmt = "SELECT film_id,title,release_year,length_category FROM tra_films"
        films_tra = pd.read_sql(sql_stmt, ses_sta_db)

        # Conexión a la base de datos de sor (data warehouse)
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'Anto2003#'
        db = 'sor'

        con_sor_db = Db_Connection(type, host, port, user, pwd, db)
        ses_sor_db = con_sor_db.start()
        dim_film_dict= {
            "film_bk":[],
            "title":[],
            "release_year":[],
            "length":[],
        }
        if not films_tra.empty:
            for bk,tit,ry,lt \
             in zip(films_tra['film_id'],films_tra['title'],films_tra['release_year'],films_tra['length_category']):
                dim_film_dict['film_bk'].append(bk)
                dim_film_dict['title'].append(tit)
                dim_film_dict['release_year'].append(ry)
                dim_film_dict['length'].append(lt)


        if dim_film_dict['film_bk']:
            df_dim_film= pd.DataFrame(dim_film_dict)
            df_dim_film.to_sql("dim_film",ses_sor_db,if_exists="append",index=False)
        print("Cargando datos de film en sor")

    except:
        traceback.print_exc()

    finally:
        pass