import asyncio
import aiofiles
import json
import pandas as pd
from async_O365 import send_email
from ops1 import load_df_with_chunk, load_df_from_db, run_pm_message


"VARS TO EDIT - csv files paths"
CONTROL_FULL_TRANSACTION = r"C:\Users\BANASIM\Desktop\jupyter_repo\BIAF\Control Full Transactions.csv"
COST_RATE_CHANGES_DATA = r"C:\Users\BANASIM\Desktop\jupyter_repo\BIAF\Feb Cost Rate Changes - data.xlsx"
CONFIG = r"C:\Users\BANASIM\PycharmProjects\BIAF\creds.json"


"VARS SETTING UP PROGRAMMATICALLY"
CLIENT_ID = None
CLIENT_SECRET = None
MAIL_BOX = None
MAIL_BOX_PASSW = None

dsn = f'Driver=SQL Server;Server=GBLON0-SQL043;DATABASE=Biaf_copy;'                              # UID={username};PWD={password}
CONN_STRING = "mssql+pyodbc://GBLON0-SQL043/Biaf_copy?driver=SQL+Server"    # "mssql+pyodbc://GBLON0-SQL043/Biaf_copy?driver=SQL+Server"


def set_params(data:json):
    if data:
        data = json.loads(data)
        CLIENT_ID = data.get("client")
        CLIENT_SECRET = data.get("secretID")
        CONN_STRING = data.get("conn_string")
        return


async def run_dataframes():
    loop = asyncio.get_event_loop()

    # async with aiofiles.open(CONFIG, mode='r') as f:
    #     content = await f.read()
    #     loop.run_in_executor(None, set_params, content)

    tasks = [loop.run_in_executor(None, load_df_with_chunk, path) for path in
             [CONTROL_FULL_TRANSACTION, COST_RATE_CHANGES_DATA]]
    dfs = await asyncio.gather(*tasks)
    # dfdb = await load_df_from_db(dsn)
    return dfs


async def db():
    return await load_df_from_db(dsn)

if __name__ == "__main__":
    try:
        """  LOGIC:  """
        # SET PARAMS
        # loop = asyncio.get_event_loop()
        # async with aiofiles.open(CONFIG, mode='r') as f:
        #     content = await f.read()
        #     loop.run_in_executor(None, set_params, content)
        # dfdb = asyncio.run(db())

        # CREATE DFs
        df1, df2 = asyncio.run(run_dataframes())

        # SET TYPE AS STRING
        df2['Employee Number'] = df2['Employee Number'].astype("string")

        # FILTER TABLE
        df2 = df2[df2['Salary Change Date'].notnull()]

        # MERGE TABLES
        merged = pd.merge(df1, df2, left_on='Employee/Supplier Number', right_on='Employee Number', how='inner')

        # DROP DUPLICATES
        merged = merged.drop_duplicates(subset=['Project Number', 'Employee/Supplier Number'])
        merged['Project Number'] = merged['Project Number'].astype("string")

        # GET LIST OF PROJECTS NUMBERS
        projects_num_list = merged['Project Number'].unique()

        # print(projects_num_list[:10])
    except Exception as e:
        #todo: logger
        print(e)
    else:
        # ASYNC QUERY FOR PROJECT MANAGER EMAIL AND SENDING EMAILS
        if any(projects_num_list):
            asyncio.run(run_pm_message(projects_num_list))




