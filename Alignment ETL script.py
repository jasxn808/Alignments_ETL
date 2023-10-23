import pyodbc 
import pandas as pd
import numpy as np

import datetime

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import NullPool

import urllib


df = pd.read_csv('... .csv')

#Converting row index 0 -> Header:
df.rename(columns=df.iloc[0], inplace=True)
df.drop(index=0, inplace=True)


#grabbing only cols we want:
df = \
    df[['Store ID', 'Week', 'District Manager', 'Regional Manager', 'Divisional Manager']]

#re-naming cols:
df = \
    df.rename(columns = \
              {'Store ID': 'Store_ATB_ID', 'District Manager':'DM', 'Regional Manager':'RM'\
                      , 'Divisional Manager':'DivM'})




#locating indexes where all values = NaN -> dropping these row indexes from df
indx = \
    df.index[df.isnull().all(1)]

#print(len(indx))  -> dropping 52 NaN rows
df = \
    df.drop(index = indx)


#dropping all cols where store = Canadian:
CA_rec = df.loc[df['Store_ATB_ID'].str.contains('C')]
CA_indx = CA_rec.index #length = 2704

df = df.drop(index = CA_indx)


##upper-casing all employee names:
for columns in df[['DM', 'RM', 'DivM']]:
  df[columns] = df[columns].str.upper()


#check if any NaNs exist, if so, replace with blank
df[df.isna().any(axis=1)] ##108 rows
df = df.fillna('')

#replacing all commas in emp_name with blank
for cols in df[['DM', 'RM', 'DivM']]:
    df[cols] = df[cols].str.replace(',', '')


## notes: account for records where CLOSED or '' exist


## some manual work - these are emp_names where format = LName FName:

df.loc[df['RM'] == '...',['RM']] = '...'
df.loc[df['DivM'] == '...',['DivM']] = '...'
df.loc[df['DivM'] == "...", ['DivM']] = '...'


# Splitting emp_name -> FName LName cols
for cols in df[['DM', 'RM', 'DivM']]:
    df[(df[cols].name + '_FName')] = df[cols].str.split(" ", n=1, expand=True)[0]
    df[(df[cols].name + '_LName')] = df[cols].str.split(" ", n=1, expand=True)[1]



#df.isnull().sum() #find out if there are any nulls
df = df.fillna('') ## for rows where CLOSED 


#because of crashing, just going to use assignment statements:
df['DM'] = df['DM_LName'] + ', ' + df['DM_FName']
df['RM'] = df['RM_LName'] + ', ' + df['RM_FName']
df['DivM'] = df['DivM_LName'] + ', ' + df['DivM_FName']


df = df[['Store_ATB_ID','Week'\
         , 'DM', 'RM','DivM']]


##Reformatting table to match SQL Server format:
## maybe make this into function?? too repetitive for now

df['Position_ID'] = ''

#temp_DM:
temp_DM = \
df.loc[(df['Week'].astype(int) >= 1) \
       & (df['Week'].astype(int) <= 52)] \
       [['Store_ATB_ID', 'Week', 'DM', 'Position_ID']]

temp_DM = temp_DM.rename(columns = {'DM': 'emp_name'})
temp_DM['Position_ID'] = '1'

#temp_RM
temp_RM = \
df.loc[(df['Week'].astype(int) >= 1) \
       & (df['Week'].astype(int) <= 52)] \
       [['Store_ATB_ID', 'Week', 'RM', 'Position_ID']]

temp_RM = temp_RM.rename(columns = {'RM': 'emp_name'})
temp_RM['Position_ID'] = '3'

#temp_DivM
temp_DivM = \
df.loc[(df['Week'].astype(int) >= 1) \
       & (df['Week'].astype(int) <= 52)] \
       [['Store_ATB_ID', 'Week', 'DivM', 'Position_ID']]

temp_DivM = temp_DivM.rename(columns = {'DivM': 'emp_name'})
temp_DivM['Position_ID'] = '5'



#Union all -> create single df
df_align = pd.concat([temp_DM, temp_RM, temp_DivM])

df_align = df_align.replace(', ','')
today = datetime.datetime.now()
df_align['FiscalYear'] = today.year



conn_str = (
    r'DRIVER={...};'
    r'SERVER=...;'
    r'DATABASE=...;'
    r'UID=...;'
    r'PWD=...;'
)
cnxn = pyodbc.connect(conn_str)

## grabbing sales codes for emps (...)
SQL = """
        SELECT 
            DISTINCT(qq.Emp_LastName + ', ' + qq.Emp_FirstName) as Emp_Name
            , qq.Sales_ID
        FROM (
            SELECT 
                CASE 
                    WHEN 
                        Emp_Name = '...' OR Emp_Name = '...' OR Emp_Name = '...'
                    THEN
                        UPPER(SUBSTRING([Emp_Name],CHARINDEX(' ', [Emp_Name]) + 1,
                            LEN([Emp_Name]) - CHARINDEX(' ', [Emp_Name])))

                    ELSE
                        SUBSTRING([Emp_Name], 1, ABS(CHARINDEX(' ', [Emp_Name]) - 1))   
                END AS Emp_FirstName
            , CASE
                    WHEN 
                        Emp_Name = '...' OR Emp_Name = '...' OR Emp_Name = '...'
                    THEN
                        UPPER(SUBSTRING([Emp_Name], 1, ABS(CHARINDEX(' ', [Emp_Name]) - 2)))
                    ELSE
                        UPPER(SUBSTRING([Emp_Name],CHARINDEX(' ', [Emp_Name]) + 1,
                            LEN([Emp_Name]) - CHARINDEX(' ', [Emp_Name])))
                END AS Emp_LastName		
            , Sales_ID
            FROM [...]
            WHERE FISCALYEAR = ?
        ) qq
    """

df_slscode = \
    pd.read_sql(SQL, cnxn, params=[today.year])


## bringing in fiscal Cal
SQL = """
        SELECT 
            FiscalMonth as FiscalPeriod
            , WeekNum as FiscalWeek
            , DATEADD(DAY, -6, DateMMDDYY) as FiscalWeekStartDate
            , DateMMDDYY as FiscalWeekEndDate
            , FiscalYear
            , CreatedDate as Created_Date
        FROM ...
        WHERE FiscalYear = ?
    """

df_fiscalcal = \
    pd.read_sql(SQL,cnxn, params=[today.year])




##Creating final dataset:
df_merged = \
  df_align.merge(df_slscode \
         ,how='left'\
         ,left_on='emp_name'\
         ,right_on='Emp_Name')\
  .merge(df_fiscalcal[['FiscalPeriod', 'FiscalWeek', 'FiscalWeekStartDate', 'FiscalWeekEndDate', 'Created_Date']]\
         ,how='left'\
         ,left_on='Week'\
         ,right_on='FiscalWeek')\
        [['FiscalYear', 'FiscalPeriod', 'FiscalWeek'\
           ,'FiscalWeekStartDate', 'FiscalWeekEndDate', 'Store_ATB_ID'\
           ,'Position_ID', 'Sales_ID', 'Emp_Name', 'Created_Date']]


df_merged['Created_By'] = '...'





dest_str = (
    r'DRIVER={...};'
    r'SERVER=...;'
    r'DATABASE=...;'
    r'UID=...;'
    r'PWD=...;'
)


conn_str = \
    urllib.parse.quote_plus(dest_str)

dest_cnxn = \
    'mssql+pyodbc:///?odbc_connect={}'.format(conn_str)

engine = \
    sqlalchemy.create_engine(dest_cnxn,poolclass=NullPool)

connection = engine.connect()
df_merged\
    .to_sql("...", engine, if_exists='append', index= False, chunksize=200)

connection.close()


