import asyncio
import aiofiles
import json
from async_O365 import send_email
from ops1 import load_df_with_chunk, load_df_from_db


"VARS TO EDIT - csv files paths"
CONTROL_FULL_TRANSACTION = r"C:\Users\BANASIM\Desktop\jupyter_repo\BIAF\Control Full Transactions.csv"
COST_RATE_CHANGES_DATA = r"C:\Users\BANASIM\Desktop\jupyter_repo\BIAF\Feb Cost Rate Changes - data.xlsx"
CONFIG = r"C:\Users\BANASIM\PycharmProjects\BIAF\creds.json"


"VARS SETTING UP PROGRAMMATICALLY"
CLIENT_ID = None
CLIENT_SECRET = None
MAIL_BOX = None
MAIL_BOX_PASSW = None

CONN_STRING = None


def set_params(data:json):
    if data:
        data = json.loads(content)
        CLIENT_ID = data.get("client")
        CLIENT_SECRET = data.get("secretID")
        CONN_STRING = data.get("conn_string")
        return

async def run_dataframes():
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(None, load_df_with_chunk, path) for path in
             [CONTROL_FULL_TRANSACTION, COST_RATE_CHANGES_DATA]]
    tasks.append(load_df_from_db(CONN_STRING))
    dfs = await asyncio.gather(*tasks)
    return dfs




if __name__ == "__main__":

    """  LOGIC:  """

    # SET PARAMS
    loop = asyncio.get_event_loop()
    async with aiofiles.open(CONFIG, mode='r') as f:
        content = await f.read()
        loop.run_in_executor(None, set_params, content)

    # CREATE DFs
    dfs = asyncio.run(run_dataframes())
    for df in dfs:
        print(df.head(4))


    # # send email notification
    # asyncio.run(send_email(CLIENT_ID, CLIENT_SECRET, 'michal.banasiak@jacobs.com'))


