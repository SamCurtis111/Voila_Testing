# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 10:12:38 2023

@author: SamCurtis
"""
#######################################################################################
## LDC STUFF
#######################################################################################
app = Retrieve_Data()
## GET THE LDC PROJECT BALANCES ##
ldc_balances = app.ldc_project_balances()
ldc_balances = ldc_balances[~(ldc_balances.Type=='AFOLU')]

## CREATE STACKED GRAPHS ##




PID = 1734

def project_balance(PID):
    issued = df_issuance[df_issuance['Project ID']==PID]
    retired = df_retirement[df_retirement['Project ID']==PID]
    balance = pd.merge(issued, retired, on=['Project ID', 'Vintage'], how="left")
    balance = balance.fillna(0)
    balance.columns = ['ID', 'Vintage', 'Issued', 'Retired']
    balance['Balance'] = balance.Issued - balance.Retired
    return balance

df_balance = project_balance(1199)


#non_afolu_balance.to_csv('C:/GitHub/non_afolu_ldc_projects.csv')




#######################################################################################
#######################################################################################
# Assigning Prices to Projects
#######################################################################################

    
    
import plotly.express as px

long_df = px.data.medals_long()

fig = px.bar(long_df, x="nation", y="count", color="medal", title="Long-Form Input")