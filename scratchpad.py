# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 10:12:38 2023

@author: SamCurtis
"""
app = Retrieve_Data()
df_issuance = app.df_issuance
ngeo_issuance = app.ngeo_issuance
ngeo_retirement = app.ngeo_retirement
ngeo_projects = app.ngeo_project_list
df_retirement = app.df_retirement
broker_markets = app.df_broker




undesirables = app.determine_undesirable_ngeos()

z_undesiable_balances = app.ngeo_undesirable_project_balances()
z_balance, z_balance_grouped = app.ngeo_undesirable_vintage_balances()

#######################################################################################
#######################################################################################
# Undesirables - projects that have traded but not at a premium to SIP price
# Or take project averages by vintage and determine outliers
#######################################################################################
broker_redd = broker_markets[broker_markets.Type.str.contains('REDD')]
broker_redd = broker_redd[~(broker_redd.Broker=='CBL')].reset_index(drop=True)
broker_redd_bid  = broker_redd[broker_redd['Price Type']=="Bid"]
broker_redd_offer  = broker_redd[broker_redd['Price Type']=="Offer"]
broker_redd_trade  = broker_redd[broker_redd['Price Type']=="Trade"]

#df = broker_redd_trade.copy()

def wma_broker_flag(df):
    offer_dict = {}
    weights10 = np.arange(1,11)
    for v in list(df.Vintage.unique()):
        sub_df = df.copy()
        sub_df = sub_df[sub_df.Vintage==v]
        sub_df['WMA10'] = sub_df['Price'].rolling(10).apply(lambda prices: np.dot(prices, weights10)/weights10.sum(), raw=True)
        sub_df['Flag'] = np.where(sub_df.Price<=(sub_df.WMA10*.7),1,0)
        sub_df['CBL'] = np.where(sub_df.Broker=='CBL',1,0)
        sub_df['Flag_Non_CBL'] = np.where((sub_df.Flag==1)&(sub_df.CBL==0),1,0)
        offer_dict[v] = sub_df
    return offer_dict
    
trade_flags = wma_broker_flag(broker_redd_trade)    





#######################################################################################
#######################################################################################
# Determine what vintages still require issuance in NGEO projects
#######################################################################################  
query = 'select * from \"VCS_Projects_Labelled\"'
df_projects = pd.read_sql(query, aws_engine)
ngeo_labelled = df_projects[df_projects['Project ID'].isin(ngeo_projects)]

vcs_statuses = list(ngeo_labelled.Status.unique())





#######################################################################################
## WHATS BEING RETIRED / VIN / METHOD / VOLUME
#######################################################################################
ngeo_retirement = app.ngeo_retirement
ngeo_retirement['YY'] = [str(i.year)[-2:] for i in ngeo_retirement['Date of Retirement']]
ngeo_retirement['YY_MM'] = [str(i.year)[-2:] +'_' + str(i.month).zfill(2) for i in ngeo_retirement['Date of Retirement']]
all_retirement = app.df_retirement

ngeos = ngeo_retirement.groupby(by=['YY_MM','Vintage']).sum()['Quantity of Units']
ngeos = ngeo_retirement.groupby(by=['YY_MM']).sum()['Quantity of Units']
ngeos = ngeo_retirement.groupby(by=['YY']).sum()['Quantity of Units']



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PID = 2539

def project_balance(PID):
    issued = df_issuance[df_issuance['Project ID']==PID]
    issued = issued.groupby(by=['Project ID','Vintage']).sum().reset_index()
    issued = issued[['Project ID','Vintage','Quantity of Units Issued']]
    retired = df_retirement[df_retirement['Project ID']==PID]
    retired = retired.groupby(by=['Project ID','Vintage']).sum()['Quantity of Units'].reset_index()
    balance = pd.merge(issued, retired, on=['Project ID', 'Vintage'], how="left")
    balance = balance.fillna(0)
    balance.columns = ['ID', 'Vintage', 'Issued', 'Retired']
    balance['Balance'] = balance.Issued - balance.Retired
    return balance

df_balance = project_balance(2539)


#non_afolu_balance.to_csv('C:/GitHub/non_afolu_ldc_projects.csv')




#######################################################################################
#######################################################################################
# Assigning Prices to Projects
#######################################################################################    
    
import plotly.express as px

long_df = px.data.medals_long()

fig = px.bar(long_df, x="nation", y="count", color="medal", title="Long-Form Input")


