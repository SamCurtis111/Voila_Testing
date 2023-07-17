# -*- coding: utf-8 -*-
"""
ACR Data to PgAdmin
Projects: https://acr2.apx.com/myModule/rpt/myrpt.asp?r=111
Issuances: https://acr2.apx.com/myModule/rpt/myrpt.asp?r=112
Retirements: https://acr2.apx.com/myModule/rpt/myrpt.asp?r=206
"""
import os
os.chdir('C:\\Users\\SamCurtis.AzureAD\\ATTUNGA CAPITAL PTY LTD\\Attunga - Documents\\Sam_Analysis\\Carbon\\analysis\\ACR')

from acr_markets import ACR_Markets
market_data = ACR_Markets()
markets = market_data.get_markets()


import pandas as pd
import numpy as np
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:iforgot23@localhost/Voluntary_Carbon')
aws_engine = create_engine('postgresql://Attunga01:875mSzNM@attunga-instance-1.c6crotlobtrk.us-east-2.rds.amazonaws.com/postgres')
engine_list = [engine, aws_engine]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## IMPORT DATA ##
query = 'select * from \"ACR_Issuance\"'
df_issuance = pd.read_sql(query, engine)
cols = ['Vintage','Total Credits Issued','Credits Issued to Project']
df_issuance[cols] = df_issuance[cols].apply(pd.to_numeric, errors='coerce') # make these columns numeric
df_issuance['Date Issued'] = pd.to_datetime(df_issuance['Date Issued'], format='%m/%d/%Y')
df_issuance['Year'] = [i.year for i in df_issuance['Date Issued']]
df_issuance['Month'] = [i.month for i in df_issuance['Date Issued']]
df_issuance = df_issuance.sort_values(by=['Date Issued']).reset_index(drop=True)

query = 'select * from \"ACR_Retirement\"'
df_retirement = pd.read_sql(query, engine)
df_retirement['Status Effective'] = pd.to_datetime(df_retirement['Status Effective'], format='%m/%d/%Y')
df_retirement['Year'] = [i.year for i in df_retirement['Status Effective']]
df_retirement['Month'] = [i.month for i in df_retirement['Status Effective']]
df_retirement = df_retirement.sort_values(by=['Status Effective']).reset_index(drop=True)

query = 'select * from \"ACR_Projects\"'
df_projects = pd.read_sql(query, engine)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## MARKET BALANCES ##
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# By Time
time_issuance = df_issuance.groupby(by=['Year','Month']).sum()['Total Credits Issued'].reset_index()
time_issuance = time_issuance.sort_values(by=['Year','Month']).reset_index(drop=True)


time_retirement = df_retirement.groupby(by=['Year','Month']).sum()['Quantity of Credits'].reset_index()
time_retirement = time_retirement.sort_values(by=['Year','Month']).reset_index(drop=True)


issuance_times = time_issuance[['Year','Month']]
retirement_times = time_retirement[['Year','Month']]
all_times = pd.concat([issuance_times,retirement_times])
all_times = all_times.drop_duplicates()

# below is wrong because it doesnt account for every time interval
time_balance = all_times.merge(time_issuance, on=['Year','Month'], how='left')
time_balance = time_balance.merge(time_retirement, on=['Year','Month'], how='left')
time_balance = time_balance.sort_values(by=['Year','Month']).reset_index(drop=True)
time_balance = time_balance.fillna(0)

time_balance['Cumulative Issuance'] = time_balance['Total Credits Issued'].cumsum()
time_balance['Cumulative Retirement'] = time_balance['Quantity of Credits'].cumsum()

time_balance = time_balance.drop(columns=['Total Credits Issued','Quantity of Credits'])
time_balance.columns = ['Year','Month','Cumulative_Issuance','Cumulative_Retirement']

time_balance['Balance'] = time_balance.Cumulative_Issuance - time_balance.Cumulative_Retirement
time_balance.to_csv('acr_balance.csv')
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# By Methodology
method_issuance = df_issuance.groupby(by=['Project Methodology/Protocol']).sum()['Total Credits Issued']
method_issuance = method_issuance.reset_index().sort_values(by=['Total Credits Issued'], ascending=False).reset_index(drop=True)

method_retirement = df_retirement.groupby(by=['Project Methodology/Protocol']).sum()['Quantity of Credits']
method_retirement = method_retirement.reset_index().sort_values(by=['Quantity of Credits'], ascending=False).reset_index(drop=True)

method_balance = method_issuance.merge(method_retirement, how='left', on='Project Methodology/Protocol')
method_balance = method_balance.fillna(0)
method_balance.columns=['Method','Issued','Retired']
method_balance['Balance'] = method_balance.Issued - method_balance.Retired
method_balance = method_balance.sort_values(by='Balance', ascending=False).reset_index(drop=True)
