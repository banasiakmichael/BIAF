import asyncio

import aiofiles
import pandas as pd
import aioodbc
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from async_O365 import send_email


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


async def load_df_from_db(dsn):
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

    async with aioodbc.connect(dsn=dsn) as conn:
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


async def run_logic(project_number):

    dsn = f'Driver=SQL Server;Server=GBLON0-SQL043;DATABASE=Biaf_copy;'
    query = f"""Select project_manager_emp_email from JA_ref_ProjectDictionary where project_number = '{project_number}';"""
    try:
        async with aioodbc.create_pool(dsn=dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    val = await cur.fetchone()
                    if val:
                        #todo: save in logger
                        async with aiofiles.open('PMs.txt', mode='a') as f:
                            # await f.write(f"for {project_number} PM is: {val[0]} " + '\n')
                            await f.write(f"{val[0]}" + '\n')
        # asyncio.run(send_email(CLIENT_ID, CLIENT_SECRET, 'michal.banasiak@jacobs.com'))

    except Exception as e:
        #todo: emial to the admin
        print(e)
    else:
        # todo: sending message service
        # loop = asyncio.get_event_loop()
        # loop.run_in_executor(None, send_email, val[0])
        ...


async def run_pm_message(projects_list):
    pm = await asyncio.gather(*[run_logic(i) for i in projects_list])
    return pm


if __name__ == "__main__":
    ...