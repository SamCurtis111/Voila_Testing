# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 15:39:03 2023

@author: SamCurtis
"""
## WHAT ARE THE KEY PROJECTS ##

# 1. What are the largest projects by:
    # total issuances
    # nature based
    
largest_projects = df_issuance.copy()
largest_projects = largest_projects.groupby(by=['Project ID','Project Country/Area','Method','Project Name']).sum()['Quantity of Units Issued'].reset_index()
largest_projects = largest_projects.sort_values(by=['Quantity of Units Issued'], ascending=False)

largest_ids = list(largest_projects['Project ID'])[:26]


largest_nature = df_issuance[df_issuance.Type=='AFOLU']
largest_nature = largest_nature.groupby(by=['Project ID','Project Country/Area','Method','Project Name']).sum()['Quantity of Units Issued'].reset_index()
largest_nature = largest_nature.sort_values(by=['Quantity of Units Issued'], ascending=False)

largest_nature_ids = list(largest_nature['Project ID'])[:26]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ARR ##
arr_markets = broker_markets.copy()
arr_markets = arr_markets[~(arr_markets['Project ID']=='VCS 2252')]
arr_markets = arr_markets[arr_markets.Type.str.contains('ARR')]
arr_markets = arr_markets[~(arr_markets.Type.str.contains('REDD'))]
arr_markets = arr_markets[~(arr_markets.Location.str.contains('China'))]
arr_markets['Offer Date'] = pd.to_datetime(arr_markets['Offer Date'])
arr_markets['Quarter'] = arr_markets['Offer Date'].dt.quarter
arr_markets = arr_markets[arr_markets.Year>=2022]

method = arr_markets.groupby(by=['Type','Vintage','Year','Quarter']).mean()['Price'].reset_index()
method = method.sort_values(by=['Year','Quarter','Type'], ascending=[False,False,True])

project = arr_markets.groupby(by=['Project ID','Vintage','Year','Quarter']).mean()['Price'].reset_index()
count = arr_markets.groupby(by=['Project ID','Vintage','Year','Quarter']).count()['Price'].reset_index()
count = arr_markets.groupby(by=['Project ID','Price Type']).count()['Price'].reset_index()
count = count.sort_values(by=['Price'], ascending=False)
pivot = count.pivot_table('Price','Project ID','Price Type')
pivot['Total'] = pivot.sum(axis=1)
pivot = pivot.fillna(0)
pivot['BidOffer'] = pivot.Bid / pivot.Offer
pivot['TradeOffer'] = pivot.Trade / pivot.Offer


## ARR;WRC ##
wrc_markets = broker_markets.copy()
wrc_markets = wrc_markets[wrc_markets.Type=='ARR; WRC']

wrc_issuance = df_issuance.copy()
wrc_issuance = wrc_issuance.dropna(subset='Method')
#wrc_issuance = wrc_issuance[wrc_issuance.Method=='ARR; WRC']
wrc_issuance = wrc_issuance[wrc_issuance.Method.str.contains('ARR')]
wrc_issuance = wrc_issuance[~(wrc_issuance.Method.str.contains('WRC'))]
issuance_projects = wrc_issuance.groupby(by=['Method','Project ID','Project Name','Project Country/Area']).sum()['Quantity of Units Issued'].reset_index()
issuance_vintages = wrc_issuance.groupby(by=['Vintage']).sum()['Quantity of Units Issued'].reset_index()





wrc_projects = df_projects.copy()
#wrc_projects = wrc_projects[wrc_projects.Method=='ARR; WRC']
wrc_projects = wrc_projects[wrc_projects.Method.str.contains('ARR')]
wrc_projects = wrc_projects[~(wrc_projects.Method.str.contains('ARR; WRC'))]
wrc_projects = wrc_projects.sort_values(by=['Status','Estimated Annual Emission Reductions'], ascending=[False,False])
projects_grouped = wrc_projects.groupby(by=['Status']).sum()['Estimated Annual Emission Reductions']

project_list = list(wrc_projects['Project ID'].unique())


sumatra = df_projects.copy()
sumatra = sumatra[sumatra.Methodology=='VM0007']



