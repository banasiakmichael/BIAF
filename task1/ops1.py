import asyncio
import aiofiles
import pandas as pd
import aioodbc
import logging
from pathlib import Path
from CONFIG import *
import numpy as np
import aiologger
from aiologger.handlers.streams import AsyncStreamHandler
from aiologger.formatters.base import Formatter
from aiologger import Logger
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from async_O365 import send_email


" logging init"
logging.basicConfig(filename='BIAF_PM_COST_RATE_notifier.log', filemode='wt', level=logging.WARNING, encoding='UTF8')
# logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%m-%Y %H:%M')
# handler = logging.handlers.QueueHandler(logging.handlers.Queue())
# logger = logging.getLogger('BIAF_PM_COST_RATE_notifier_ops1')
# logger.setLevel(logging.INFO)
# logger.addHandler(handler)



""" async logging module settings """
# async def setup_logger():
#     formatter = Logger()
#     handler = AsyncStreamHandler(formatter=formatter)
#     logger = aiologger.Logger.with_default_handlers()
#     await do_nothing()
#     return logger


cols = {}


def load_df_with_chunk(path:str)->pd.DataFrame:
    """
    it allows to load large numbers of rows to be loaded without loading the entire dataset into memory

    :param path: string to csv or xlsx
    :flag path:
        csv -> load csv file
    :return: pandas dataframe

    """

    suffix = Path(path).suffix

    df = None
    chunksize:int = 10000
    dfs =[]

    if suffix == ".csv":
        for chunk in pd.read_csv(path, chunksize=chunksize):
            dfs.append(chunk)
        df = pd.concat(dfs, ignore_index=True)
        return df
    if suffix == ".xlsx":
        df = pd.read_excel(path)
        return df


async def load_df_from_db(dns):
    """
    load table with fetching data with chunks
    :param conn_string: string to connect to the database
    :return: df
    """
    chunk_size = 10000
    # project_name, project_number, project_manager_emp_email, project_end_date, project_start_date, project_manager_emp_email, project_manager_emp_num
    query = """
        Select *
          from JA_ref_ProjectDictionary"""
    df = pd.DataFrame

    async with aioodbc.connect(dsn=dns) as conn:
        dfs = []
        async with conn.cursor() as cursor:
            await cursor.execute(query)
            while True:
                rows = await cursor.fetchmany(chunk_size)
                if not rows:
                    break
                df = pd.DataFrame(rows, columns=[column for column in cursor.description])   # [0]
                dfs.append(df)
    if len(dfs) > 0:
        final_df = pd.concat(dfs, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()


async def run_logic(project_number, data_frame:pd.DataFrame):
    # dsn = f'Driver=SQL Server;Server=GBLON0-SQL043;DATABASE=Biaf_copy;'
    query = f"""Select project_manager_emp_email from JA_ref_ProjectDictionary where project_number = '{project_number}';"""
    try:
        async with aioodbc.create_pool(dsn=dns) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    val = await cur.fetchone()
                    if val:
                        cols.update({f'{project_number}': f'{val[0]}'})
                        #async with aiofiles.open('PMs.txt', mode='a') as f:
                            #await f.write(f"{project_number}, {val[0]} " + '\n')
                            # await f.write(f"{val[0]}" + '\n')



    except Exception as e:
        #todo: emial to the admin
        print(e)
    else:
        # EMAIL DATA PROCESSING
        # asyncio.run(send_email(CLIENT_ID, CLIENT_SECRET, 'FROM@jacobs.com', 'passw', val[0]))

        # loop = asyncio.get_event_loop()
        # loop.run_in_executor(None, send_email, val[0])
        ...
    finally:
        ...
        # await logger.shutdown()


async def sending_emails(df:pd.DataFrame):
    sent = await asyncio.gather(*[send_email(CLIENT_ID, CLIENT_SECRET, 'FROM@jacobs.com', 'passw', i, df) for i in df['Project Manager'].unique()])
    return sent


async def run_pm_message(projects_list, data_frame):
    # logger = Logger.with_default_handlers()
    pm = await asyncio.gather(*[run_logic(i, data_frame) for i in projects_list])
    #await logger.info(f"done !!!")
    # await logger.shutdown()
    return pm, cols


if __name__ == "__main__":
    ...