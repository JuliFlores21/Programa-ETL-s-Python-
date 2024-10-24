from util.db_connection import Db_Connection
import traceback
import pandas as pd

def extraer_dates():
    try:
        filename = './csvs/dates.csv'
        dates = pd.read_csv(filename)
        return dates
    except:
        traceback.print_exc()
    finally:
        pass