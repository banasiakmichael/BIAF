"""

 CONFIG: covers vars, cons, path etc..

    creds.json --> development mode only - sensitive data storage

"""
import json



#MAILBOX CREDS
CLIENT_ID = None
CLIENT_SECRET = None
MAIL_BOX = None
MAIL_BOX_PASSW = None


with open('creds.json', 'r+') as file:
    data = json.load(file)

    # PATHS
    CONTROL_FULL_TRANSACTION = r"C:\Users\BANASIM\Desktop\jupyter_repo\BIAF\Control Full Transactions.csv"
    COST_RATE_CHANGES_DATA = r"C:\Users\BANASIM\Desktop\jupyter_repo\BIAF\Feb Cost Rate Changes - data.xlsx"
    CONFIG = r"C:\Users\BANASIM\PycharmProjects\BIAF\creds.json"

    CLIENT_ID = data.get("client")
    CLIENT_SECRET = data.get("secretID")
    CONN_STRING = data.get("conn_string")


#db connection string
if data:
    server = data.get('server')
    database = data.get('database')
    dns = data.get('dns')
    CONN_STRING = data.get('conn_string')

    #dsn = f'Driver=SQL Server;Server=GBLON0-SQL043;DATABASE=Biaf_copy;'                              # UID={username};PWD={password}
    # CONN_STRING = "mssql+pyodbc://GBLON0-SQL043/Biaf_copy?driver=SQL+Server"