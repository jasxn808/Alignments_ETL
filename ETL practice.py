## ... ETL Practice:


import pandas as pd 
import numpy as np
#import pyodbc
import urllib

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import NullPool




df = \
    pd.read_csv('....csv')

## transformation:

df['Country'] =  df['...'].str[:2]

df['DATE_PURCHASE'] = pd.to_datetime(df['DATE_PURCHASE']).dt.date

df['...'] = \
    ('000' + df['...'].astype(str)).str.slice(start=-3)

df['PURCHASE_TYPE'] = \
    np.where(df['RECEIPT_ID'].astype(str).str.contains('-'), 1, 2)

df['PURCHASE_EMAIL'] = np.nan


df.rename\
    (columns = {'...':'Store', 'TOTAL':'Purchase_Retail', 'DATE_PURCHASE':'Purchase_Date'\
                ,'RECEIPT_ID': 'Purchase_OrderNo'}, inplace=True)

df = df.query('Country.isin(["..", "..", "..", ".."])')


df = \
    df[
		['Store', 'Purchase_Retail', 'Purchase_Date', 'PURCHASE_EMAIL' ,'Purchase_OrderNo', 'PURCHASE_TYPE']
	  ]

# /**
# De-duplicating data before INSERT
# **/

conn_str = (
    r'DRIVER={...};'
    r'SERVER=...;'
    r'DATABASE=...;'
    r'UID=...;'
    r'PWD=...;'
)


conn_str = \
    urllib.parse.quote_plus(conn_str)

dest_cnxn = \
    'mssql+pyodbc:///?odbc_connect={}'.format(conn_str)

engine = \
    sqlalchemy.create_engine(dest_cnxn,poolclass=NullPool)

connection = engine.connect()

SQL = \
    """
        SELECT * 
        FROM ...
        WHERE Purchase_Date >= '2023-10-01'
    """

df_dest = \
    pd.read_sql(SQL, engine)



df_merged = \
df_dest.merge(df\
              , indicator=True, how='outer')\
                .query('_merge=="left_only"').drop('_merge', axis=1)\
        [['Store', 'Purchase_Retail','Purchase_Date','Purchase_Email', 'Purchase_OrderNo', 'Purchase_Type']]



# /** 
# Insert cleaned flat file data from CrowdTwist into main Purchase Table for reporting
# **/ 
conn_str = (
    r'DRIVER={...};'
    r'SERVER=...;'
    r'DATABASE=...;'
    r'UID=...;'
    r'PWD=...;'
)

conn_str = \
    urllib.parse.quote_plus(conn_str)

dest_cnxn = \
    'mssql+pyodbc:///?odbc_connect={}'.format(conn_str)

engine = \
    sqlalchemy.create_engine(dest_cnxn,poolclass=NullPool)

connection = engine.connect()


df_merged.to_sql\
    ("...", engine, if_exists='append', index= False)





#creating import table to DB:
## this locks up my DB...
# curr.execute \
#     ("""
#         IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Stash_Daily_Purchases_Import')
#         BEGIN 
#             CREATE TABLE Stash_Purchases_Import(
                # [Purchase_DateStoreStyle] [varchar](255) NULL,
                # [Purchase_LineRetail] [float] NULL,
                # [Purchase_Date] [varchar](50) NULL,
                # [Purchase_Email] [varchar](255) NULL,
                # [Purchase_OrderNo] [varchar](255) NULL,
                # [Purchase_Type] [varchar](50) NULL,
                # [flatfile_source] [varchar](255) NULL
#             )
#         END 
#     """
#     )
      










