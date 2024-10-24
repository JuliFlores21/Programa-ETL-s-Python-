# this file is a kind of python startup module used for manual unit testing

#from util.db_connection import Db_Connection
import traceback
import pandas as pd
from extract.ext_countries import extraer_countries
from extract.ext_store import extraer_stores
from extract.per_staging import persistir_staging
from transform.tra_store import transformar_stores
from extract.ext_address import extraer_address
from extract.ext_cities import extraer_cities
from load.load_stores import cargar_stores
from extract.ext_date import extraer_dates
from extract.ext_film import extraer_films
from extract.ext_inventory import extraer_inventories
from transform.tra_films import transformar_films
from transform.tra_date import transformar_dates
from load.load_dates import cargar_dates
from load.load_films import cargar_films
from transform.tra_inventory import transformar_inventory
from load.load_inventory import cargar_inventory

try:
    #con_db = Db_Connection('mysql','localhost','3306','root','Anto2003#','oltp')
    #ses_db = con_db.start()
    #if ses_db == -1:
     #   raise Exception("El tipo de base de datos dado no es válido")
    #elif ses_db == -2:
     #   raise Exception("Error tratando de conectarse a la base de datos ")
    
    #databases = pd.read_sql ('SELECT COUNT(*) FROM oltp.customer', ses_db)
    #print (databases)

    print("Extrayendo datos de countries desde csv")
    countries = extraer_countries()
    print("persistiendo en staging datos de countries")
    persistir_staging(countries,'ext_country')

    print("Extrayendo datos de store desde DB")
    stores = extraer_stores()
    print("persistiendo en staging datos de stores")
    persistir_staging(stores,'ext_store')

    print("Extrayendo datos de city desde DB")
    city = extraer_cities()
    print("persistiendo en staging datos de city")
    persistir_staging(city,'ext_city')

    print("Extrayendo datos de address desde DB")
    address = extraer_address()
    print("persistiendo en staging datos de address")
    persistir_staging(address,'ext_address')

    print("Transformando datos de store en el staging")
    tra_stores = transformar_stores()
    print("persistiendo en staging datos transformados de stores")
    persistir_staging(tra_stores,'tra_store')

    print("Extrayendo datos de date desde csv")
    dates = extraer_dates()
    print("Persistiendo en staging datos de date")
    persistir_staging(dates,'ext_date')

    print("Extrayendo datos de film desde oltp")
    films = extraer_films()
    print("Persistiendo en staging datos de film")
    persistir_staging(films,'ext_film')

    print("Extrayendo datos de inventory desde oltp")
    inventory = extraer_inventories()
    print("Persistiendo en staging datos de inventory")
    persistir_staging(inventory,'ext_inventory')

    print("Transformando datos de date")
    tra_date= transformar_dates()
    print("Persistiendo en staging datos transformados de film")
    persistir_staging(tra_date,'tra_date')

    print("Transformando datos de film")
    tra_film= transformar_films()
    print("Persistiendo en staging datos transformados de film")
    persistir_staging(tra_film,'tra_films')

    # print("Cargando datos date en sor")
    # cargar_dates()

    # print("Cargando datos film en sor")
    # cargar_films()
    # print("Extrayendo datos transformados inventory")
    # inventory_tra= transformar_inventory()
    # print("Persistiendo en staging datos transformados inventory")
    # persistir_staging(inventory_tra,'tra_inventory')

    # print("Cargando datos inventory en fact_inventory")
    # cargar_inventory()

except:
    traceback.print_exc()
finally:
    pass

 #   ses_db_oltp = con_db.stop()