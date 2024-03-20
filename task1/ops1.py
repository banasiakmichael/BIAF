import asyncio
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine


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


async def load_df_from_db_middleware(conn_string, chunksize=10000):
    engine = create_engine(conn_string, echo=True)
    async with engine.connect() as conn:
        result = await conn.execute("""
        Select project_name, project_number, project_manager_emp_email, project_end_date, project_start_date, project_manager_emp_email, project_manager_emp_num
          from JA_ref_ProjectDictionary """)
        while True:
            rows = await result.fetchmany(chunksize)
            if not rows:
                break
            yield rows

async def load_df_from_db(conn_string):
    dfs = []
    async for df in load_df_from_db_middleware(conn_string):
        dfs.append(df)
    df_final = pd.concat(dfs, ignore_index=True)
    return df_final


def from_control_transaction():
    ...



if __name__ == "__main__":
    ...