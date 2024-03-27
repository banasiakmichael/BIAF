import asyncio
import aiofiles
import json
import logging
import pandas as pd
from CONFIG import *
from async_O365 import send_email
from ops1 import load_df_with_chunk, load_df_from_db, run_pm_message, sending_emails


logging.basicConfig(filename='BIAF_PM_COST_RATE_logger.log', filemode='wt', level=logging.WARNING, encoding='UTF8')
# logger.setLevel(logging.INFO)

async def run_dataframes():
    """
    async task gathering for dataframes.
    Method uses Future classe
    :return: pandas dataframe : dfs[0], dfs[1]...
    """
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
    """
    ms sql async loader --> test only
    :return:
    """
    return await load_df_from_db(dns)


"   **************************************** RUN SCRIPT *********************************************************  "


if __name__ == "__main__":
    """
        main: 
            - creating dataframes from files
            - dataframes operations to get projects lists:
                -> async querying ms sql for Project Managers email address. Then: sending emails
                -> adding PMs to txt file - TEST STAGE ONLY
                -> creating xls file  - TEST STAGE ONLY
    """
    try:
        """  LOGIC:  """

        # CREATE DFs
        df1, df2 = asyncio.run(run_dataframes())

        # create df from ProjectDictionary tab
        #...

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

        df_to_excel = merged[['Project Number', 'Project Description', 'Employee/Supplier Number', 'Employee/Supplier Name']].copy()

        # print(projects_num_list[:10])
    except Exception as e:
        print(e)
    else:
        # ASYNC QUERY FOR PROJECT MANAGER
        if any(projects_num_list):
            col = asyncio.run(run_pm_message(projects_num_list, df_to_excel))
            df_to_excel['Project Manager'] = df_to_excel['Project Number'].map(col[1])
            df_to_excel = df_to_excel[['Project Number', 'Project Description', 'Employee/Supplier Name', 'Project Manager']].copy()

            # with pd.ExcelWriter('output.xlsx') as excel_writer:
            #     df_to_excel.to_excel(excel_writer, sheet_name='data', index=False)

            # ASYNC ENDING EMAILS
            asyncio.run(sending_emails(df_to_excel))






