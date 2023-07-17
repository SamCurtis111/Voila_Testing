# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 10:12:38 2023

@author: SamCurtis
"""
from fuzzywuzzy import fuzz
app = Retrieve_Data()

df_projects = app.df_projects
df_issuance = app.df_issuance
ngeo_issuance = app.ngeo_issuance
ngeo_retirement = app.ngeo_retirement
ngeo_projects = app.ngeo_project_list
df_retirement = app.df_retirement
broker_markets = app.df_broker


"""
CORSIA 
"""
import datetime
projects = df_projects.copy()
projects = projects[projects.Type=='Non_AFOLU']

projects = projects[~(projects['Crediting Period Start Date'] < datetime.date(year=2016,month=1,day=1))]
projects = projects[~(projects.Status.str.contains('Rejected'))]
projects = projects[~(projects.Method.str.contains('Cookstoves'))]

project_list = list(projects['Project ID'].unique())

registered_projects = projects[projects.Status=='Registered']
registered_projects = list(registered_projects['Project ID'].unique())


annual_emissions = sum(projects['Estimated Annual Emission Reductions'])


## NEED TO SCRAPE VERRA TO FIND ALL PROJECTS WITH AN SDG



undesirables = app.determine_undesirable_ngeos()

z_undesiable_balances = app.ngeo_undesirable_project_balances()
z_balance, z_balance_grouped = app.ngeo_undesirable_vintage_balances()


#######################################################################################
#######################################################################################
# Analysis of Affo projects
# ARR projects only (no arr;redd, arr;wrc, etc)
#######################################################################################
affo_types = list(df_projects['AFOLU Activities'].unique())

affo_projects = df_projects[df_projects['AFOLU Activities']=='ARR']
affo_projects = list(affo_projects['Project ID'].unique())

affo_issuance = df_issuance[df_issuance['Project ID'].isin(affo_projects)]
affo_retirement = df_retirement[df_retirement['Project ID'].isin(affo_projects)]

issuance_affo = affo_issuance.groupby(by=['Project Country/Area']).sum()['Quantity of Units Issued'].reset_index()
retirement_affo = affo_retirement.groupby(by=['Project Country/Area']).sum()['Quantity of Units'].reset_index()
affo_balance = issuance_affo.merge(retirement_affo, on=['Project Country/Area'], how='left')
affo_balance.columns = ['Country','Issued', 'Retired']
affo_balance = affo_balance.fillna(0)
affo_balance['Balance'] = affo_balance.Issued - affo_balance.Retired
affo_balance = affo_balance.sort_values(by='Balance', ascending=False).reset_index(drop=True)

## SUMMARY OF GEOGRAPHICAL BROKER MARKETS
affo_projects_broker = ['VCS '+str(i) for i in affo_projects]
affo_broker = broker_markets[broker_markets['Project ID'].isin(affo_projects_broker)]

affo_broker = affo_broker.groupby(by=['Location','Price Type']).count()['Price'].reset_index()
affo_broker = affo_broker.pivot_table('Price','Location','Price Type')
affo_broker = affo_broker.fillna(0).reset_index()
affo_broker = affo_broker.sort_values(by=['Trade'], ascending=False).reset_index(drop=True)


## VINTAGE ANALYSIS (balances & broker data)
vin_issuance = affo_issuance.groupby(by=['Vintage']).sum()['Quantity of Units Issued'].reset_index() 
vin_retirement = affo_retirement.groupby(by=['Vintage']).sum()['Quantity of Units'].reset_index()
vin_balance = vin_issuance.merge(vin_retirement, on=['Vintage'], how="left")
vin_balance.columns = ['Vintage','Issued','Retired']
vin_balance['Balance'] = vin_balance.Issued - vin_balance.Retired

vin_broker = broker_markets[broker_markets['Project ID'].isin(affo_projects_broker)]
vin_broker = vin_broker.groupby(by=['Vintage','Price Type']).count()['Price'].reset_index()
vin_broker = vin_broker.pivot_table('Price','Vintage','Price Type')
vin_broker = vin_broker.fillna(0).reset_index()


# TIME SERIES OF BROKER MARKETS
affo_markets = broker_markets[broker_markets['Project ID'].isin(affo_projects_broker)]
affo_trades = affo_markets[affo_markets['Price Type']=='Trade']
affo_bids = affo_markets[affo_markets['Price Type']=='Bid']
affo_offers = affo_markets[affo_markets['Price Type']=='Offer']

trades = affo_trades.groupby(by=['Year','Month']).mean()['Price'].reset_index()
trades.colums = ['Year','Month','Price_Trade']
offers = affo_offers.groupby(by=['Year','Month']).mean()['Price'].reset_index()
offers.colums = ['Year','Month','Price_Offer']
bids = affo_bids.groupby(by=['Year','Month']).mean()['Price'].reset_index()
bids.colums = ['Year','Month','Price_Bid']

aggregate = trades.merge(bids, on=['Year','Month'], how='left')
aggregate = aggregate.merge(offers, on=['Year','Month'], how='left')


with pd.ExcelWriter('C:/GitHub/CSV_Outputs/affo_markets.xlsx') as writer:
    affo_trades.to_excel(writer, sheet_name='Trades')
    affo_bids.to_excel(writer, sheet_name='Bids')
    affo_offers.to_excel(writer, sheet_name='Offers')


## PIPELINE OF NEW AFFO
affo_project_raw = df_projects[df_projects['AFOLU Activities']=='ARR']
status_types = list(affo_project_raw.Status.unique())
drop_types = ['Rejected by Administrator','Units Transferred fromo Approved GHG Program','Registered','Inactive']
keep_types = [i for i in status_types if i not in drop_types]

affo_pipeline = affo_project_raw[affo_project_raw.Status.isin(keep_types)]
affo_pipeline['Vin_Start'] = [i.year for i in affo_pipeline['Crediting Period Start Date']]
affo_pipeline['Vin_End'] = [i.year for i in affo_pipeline['Crediting Period End Date']]


z = df_issuance[df_issuance['Project ID']==1847]
z = z.groupby(by=['Vintage']).sum()['Quantity of Units Issued'].reset_index()




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
# What projects / characteristics are being bid
#######################################################################################
def groupon(df, price_type='Offer', kind='Project ID', include_CBL='Yes'):
    if include_CBL=='No':
        sub_df = df[df.Broker!='CBL']
    if include_CBL=='Yes':
        # Only include CBL data over 10kt
        df_cbl = df[(df.Broker=='CBL')&(df.Volume>=10000)]
        df_other = df[df.Broker!='CBL']
        sub_df = pd.concat([df_cbl, df_other])
        sub_df = sub_df.reset_index(drop=True)
    sub_df = sub_df[(sub_df['Price Type']== price_type)]
    sub_df = sub_df.drop_duplicates(subset=['Offer Date','Project ID','Price','Price Type','Volume','Broker','Year','Month','Vintage'])
    
    df_raw = sub_df.groupby(by=[kind,'Year','Month']).count()['Price Type'].reset_index()
    df_raw_price = sub_df.groupby(by=[kind,'Year','Month']).mean()['Price'].reset_index()
    df_raw = df_raw.merge(df_raw_price, on=[kind, 'Year','Month'], how="left")
    df_raw.columns = [kind, 'Year','Month','Count','Avg_Price']
    df_raw = df_raw.sort_values(by=['Year','Month','Count'], ascending=[False,False,False])
    #df_raw = df_raw[df_raw['Count']>1]
    df_raw['Avg_Price'] = [round(i,2) for i in df_raw.Avg_Price]
    
    df_raw.columns = [kind,'Year','Month', price_type+'_Count', 'Avg '+price_type+' Price']
    return df_raw

def summary(df, datatype, CBL='Yes'):
    bids = groupon(df, price_type='Bid', kind=datatype, include_CBL=CBL)
    offers = groupon(df, price_type='Offer', kind=datatype, include_CBL=CBL)
    trades = groupon(df, price_type='Trade', kind=datatype, include_CBL=CBL)
    
    sub = pd.merge(bids, offers, how='outer', on=[datatype,'Year','Month'])
    df_agg = pd.merge(sub, trades, how='outer', on=[datatype,'Year','Month'])
    
    df_agg = df_agg[['Year','Month',datatype,'Bid_Count','Offer_Count','Trade_Count','Avg Bid Price','Avg Offer Price','Avg Trade Price']]
    df_agg = df_agg.sort_values(by=['Year','Month',datatype], ascending=[False,False,True])
    df_agg = df_agg.fillna(0)
    
    df_agg['Bid:Offer'] = df_agg['Bid_Count'] / df_agg['Offer_Count']
    df_agg['Bid:Offer'] = [round(i,2) for i in df_agg['Bid:Offer']]
    return df_agg

def vwap(df):
    df = df.groupby(by=['Year','Month']).mean()[['Price']].reset_index()
    df['AvgPrice'] = df.Price
    df['AvgPrice'] = round(df.AvgPrice,2)
    df = df.drop(columns='Price')
    df = df.sort_values(by=['Year','Month'], ascending=[True,True]).reset_index(drop=True)
    return df

def price_summary(df, CBL='Yes'):
    if CBL=='Yes':
        # Only include CBL data over 1kt
        df_cbl = df[(df.Broker=='CBL')&(df.Volume>=1000)]
        df_other = df[df.Broker!='CBL']
        df = pd.concat([df_cbl, df_other])
    
    bids = df[df['Price Type']=='Bid']
    bids = vwap(bids)
    bids.columns = ['Year','Month','Bid']
    offers = df[df['Price Type']=='Offer']
    offers = vwap(offers)
    offers.columns = ['Year','Month','Offer']
    trades = df[df['Price Type']=='Trade']
    trades = vwap(trades)
    trades.columns = ['Year','Month','Trade']
    
    sub = trades.merge(bids, on=['Year','Month'], how="left")
    sub = sub.merge(offers, on=['Year','Month'], how='left')

    return sub
    
df_type = summary(broker_markets, datatype='Type', CBL='No')
df_type = df_type.sort_values(by=['Year','Month','Bid_Count'], ascending=[False,False,False])

df_vintage = summary(broker_markets, datatype='Vintage', CBL='No')

df_project = summary(broker_markets, datatype='Project ID', CBL='No')
df_project = df_project.sort_values(by=['Year','Month','Bid_Count'], ascending=[False,False,False])

df_prices = price_summary(broker_markets, CBL='Yes')

## Vintage ##
# TO DO - split out renewables
x = df_vintage.copy()    
x = x.groupby(by=['Year','Vintage']).sum()
x = x.reset_index()
x = x[['Year','Vintage','Bid_Count','Offer_Count','Trade_Count']]

z = df_vintage.copy()    
z = z.groupby(by=['Year','Vintage']).mean()
z = z.reset_index()
z = z[['Year','Vintage','Avg Bid Price','Avg Offer Price','Avg Trade Price']]

xz = x.merge(z, on=['Year','Vintage'], how="left")
##




## Most Bid Projects ##
project = df_project.copy()
top_projects = project.groupby(by=['Project ID']).sum()
#top_projects = top_projects[(top_projects.Offer_Count>=2) & (top_projects.Bid_Count>=1)]
top_projects = top_projects[['Bid_Count','Offer_Count','Trade_Count']]
top_projects['Bid:Offer'] = top_projects['Bid_Count'] / top_projects['Offer_Count']
top_projects['Bid:Offer'] = [round(i,2) for i in top_projects['Bid:Offer']]
top_projects = top_projects.reset_index()

# Merge top projects with their names
#copy_projects = df_projects.copy()
#copy_projects['Project ID'] = ['VCS '+str(i) for i in copy_projects['Project ID']]
#copy_projects = copy_projects[['Project ID','Project Name','Method','Type','Country/Area']]

broker_markets_sub = broker_markets.copy()
broker_markets_sub = broker_markets_sub[['Project ID','Name','Location','Type']]
broker_markets_sub = broker_markets_sub.drop_duplicates()

top_projects = top_projects.mergeiforgot23(broker_markets_sub, on='Project ID',how="left")
top_projects = top_projects.sort_values(by=['Bid_Count','Trade_Count'], ascending=[False,False])
##

## Most bid projects (monthly aggregation) ##
# Drop projects that have no bids
project_sub = project.copy()
project_sub = project_sub.groupby(by=['Project ID']).sum()
project_sub = project_sub[project_sub.Bid_Count>=5]
project_sub = project_sub.reset_index()
keep_projects = project_sub['Project ID']

project_monthly = project.copy()
project_monthly = project_monthly[project_monthly['Project ID'].isin(keep_projects)]
project_monthly = project_monthly.groupby(by=['Year','Month','Project ID']).sum()
project_monthly = project_monthly.reset_index()
project_monthly = project_monthly.sort_values(by=['Project ID','Year','Month'])
project_monthly['Y'] = [str(i) for i in project_monthly.Year]
project_monthly['M'] = [str(i) for i in project_monthly.Month]
project_monthly['M'] = project_monthly.M.str.zfill(2)
project_monthly['Y_M'] = project_monthly.Y+project_monthly.M
project_monthly = project_monthly.sort_values(by=['Year','Month','Bid_Count'], ascending=[True,True,False])

bid_monthly = project_monthly[['Y_M','Project ID','Bid_Count']]
bid_monthly = pd.pivot_table(bid_monthly, values='Bid_Count', index='Y_M', columns=['Project ID'], fill_value=0).reset_index()

    
## Bids / Offers Monthly
monthly = project.groupby(by=['Year','Month']).sum()
monthly = monthly[monthly.Offer_Count>0]
monthly = monthly[['Bid_Count','Offer_Count','Trade_Count']]
monthly['Bid:Offer'] = monthly.Bid_Count / monthly.Offer_Count

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

PID = 2250

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

df_balance = project_balance(PID)


#non_afolu_balance.to_csv('C:/GitHub/non_afolu_ldc_projects.csv')




#######################################################################################
#######################################################################################
# Assigning Prices to Projects
#######################################################################################    
    




#######################################################################################
#######################################################################################
# Assessing seasonality in demand / retirement
####################################################################################### 
def seasonality_trends(df):

    
df_retirement = df_retirement.drop_duplicates()
df_retirement['Year'] = [i.year for i in df_retirement['Date of Retirement']]
df_retirement['Month'] = [i.month for i in df_retirement['Date of Retirement']]

df_retirement = df_retirement.groupby(by=['Year','Month']).sum()['Quantity of Units']

z = df_retirement.copy()
z = z.pivot_table





#######################################################################################
#######################################################################################
# RETIREMENT ANALYSIS
# who is retiring
# what methods
# what vintages
# what projects
####################################################################################### 
r = df_retirement.copy()
r['Year'] = [i.year for i in r['Date of Retirement']]
r['Month'] = [i.month for i in r['Date of Retirement']]
r['Vintage'] = [i.year for i in r['From Vintage']]

racc = r.groupby(by=['Year','Month','Account Holder']).sum()['Quantity of Units'].reset_index()
racc = racc.sort_values(by=['Year','Month','Quantity of Units'], ascending=[False,False,False])

rvin = r.groupby(by=['Year','Month']).mean()['Vintage'].reset_index()

rcount = r.groupby(by=['Year','Month']).count()['Vintage'].reset_index()
rcount.columns = ['Year','Month','Count']

rets = rvin.merge(rcount, on=['Year','Month'], how="left")
rets.to_csv('C:\\Users\\SamCurtis.AzureAD\\Downloads\\Average_Vintage.csv')


r_pivot = r.groupby(by=['Year','Month','Method']).count()['Project ID'].reset_index()
p = pd.pivot_table(r_pivot, values='Project ID', index=['Year','Month'], columns=['Method'],fill_value=0).reset_index()
p.to_csv('C:\\Users\\SamCurtis.AzureAD\\Downloads\\Method_retirements.csv')

r_2380 = r[r['Account Holder']=='2380']





#######################################################################################
## ANALYSIS OF A SPECIFIC PID
# vintage balances
# issuance / retirement monthly heatmap (bqnt)
# issuances
# retirements
    ## does it get frequently retired (monthly table)
    ## by who 
# trade data
# rating?
# non-quant data
# articles
#######################################################################################
class Project_Analysis:
    def __init__(self, PID):
        self.ID = PID
        self.broker_ID = ['VCS ' + str(i) for i in self.ID]
        
        self.project_summary = df_projects[df_projects['Project ID'].isin(self.ID)]
        self.annual_abatement = self.project_summary['Estimated Annual Emission Reductions']
        
        self.broker_data = broker_markets[broker_markets['Project ID'].isin(self.broker_ID)]
        
        self.issuance = df_issuance
        self.issuance = self.issuance[self.issuance['Project ID'].isin(self.ID)]
        self.issuance['Year'] = [i.year for i in self.issuance['Issuance Date']]
        self.issuance['Month'] = [i.month for i in self.issuance['Issuance Date']]
        
        self.retirement = df_retirement
        self.retirement = self.retirement[self.retirement['Project ID'].isin(self.ID)]
        #self.retirement = retirement[retirement['Project ID']==prid]
        self.retirement['Year'] = [i.year for i in self.retirement['Date of Retirement']]
        #self.retirement['Year'] = [i.year for i in retirement['Date of Retirement']]
        self.retirement['Month'] = [i.month for i in self.retirement['Date of Retirement']]
        #self.retirement['Month'] = [i.month for i in retirement['Date of Retirement']]
        
    def project_balance(self):
        issued = self.issuance.groupby(by=['Project ID','Vintage']).sum().reset_index()
        issued = issued[['Project ID','Vintage','Quantity of Units Issued']]

        retired = self.retirement.groupby(by=['Project ID','Vintage']).sum()['Quantity of Units'].reset_index()
        balance = pd.merge(issued, retired, on=['Project ID', 'Vintage'], how="left")
        balance = balance.fillna(0)
        balance.columns = ['ID', 'Vintage', 'Issued', 'Retired']
        balance['Balance'] = balance.Issued - balance.Retired
        return balance
    
    def issuance_retirement_tables(self):
        retired = self.retirement.groupby(by=['Year','Month']).sum()['Quantity of Units'].reset_index()
        retired = retired.pivot_table('Quantity of Units', 'Month','Year').reset_index()
        retired = retired.fillna(0)
        
        issued = self.issuance.groupby(by=['Year','Month']).sum()['Quantity of Units Issued'].reset_index()
        issued = issued.pivot_table('Quantity of Units Issued', 'Month','Year').reset_index()
        issued = issued.fillna(0)
        return issued, retired
    
    #~~~~~~~~~~~~~~~~~~~~~~~
    # Identifying the top retirees
    #~~~~~~~~~~~~~~~~~~~~~~
    ## Create a fuzzy lookup function
    def match_names(self, name, list_names, min_score=0):
        scores = pd.DataFrame()
        scores['Name'] = list_names
        ratio = []
        for i in scores.Name:
            score = fuzz.ratio(name, i)
            ratio.append(score)
        scores['Ratio'] = ratio
        scores = scores[scores.Ratio>=75]
        return scores
    
    # Use fuzzy function to merge names
    def top_retirees(self):
        account_types = ['Beneficial Owner','Account Holder']
        results = []
        
        for acc in account_types:
            retirees = self.retirement.groupby(by=['{}'.format(acc),'Year']).sum()['Quantity of Units'].reset_index()
            retirees = project.retirement.groupby(by=['{}'.format(acc),'Year']).sum()['Quantity of Units'].reset_index()
            #retirees = self.retirement.groupby(by=['Account Holder','Year']).sum()['Quantity of Units'].reset_index()
            #retirees = project.retirement.groupby(by=['Account Holder','Year']).sum()['Quantity of Units'].reset_index()
            retirees.columns = ['Name','Retirement Year','Qty Retired']     
            
            owners = list(retirees['Name'].unique())
            
            match_dict = {}
            best_name = {}
            
            for i in owners:
                matches = self.match_names(i, owners)
                #matches = project.match_names(i, owners)
                if len(matches) > 1:
                    matches = matches.merge(retirees, on=['Name'], how="left")
                    match_dict[i] = matches
                    
                    year_retired = matches.groupby(by=['Retirement Year']).sum().reset_index()  
                    total_retired = matches['Qty Retired'].sum()
                    largest_value = max(matches['Qty Retired'])
                    name = matches[matches['Qty Retired']==largest_value].reset_index(drop=True)
                    name = name.Name[0]
                    year_retired['Name'] = name 
                    year_retired = year_retired[['Name','Retirement Year','Qty Retired']]
                    best_name[name] = year_retired
                    
                else:
                    final_frame = retirees.copy()
                    final_frame.columns = ['Name','Retirement Year','Qty Retired']
                    
                    retiree_totals = final_frame.groupby(by=['Name']).sum()['Qty Retired'].reset_index()
                    retiree_totals.columns = ['Name','Total']
                    
                    final_frame = final_frame.pivot_table('Qty Retired','Name','Retirement Year')
                    
                    final_frame = final_frame.merge(retiree_totals, on=['Name'], how='left')
                    pass
                
            if len(list(match_dict)) > 1:
                d = { k: v.set_index('Name') for k, v in best_name.items()}    # Conver the Dict into a DF
                
                df = pd.concat(d)
                df = df.droplevel(0)
                df = df.reset_index()
                
                dropnames = list(match_dict)
                retirees = retirees[~retirees.Name.isin(dropnames)]
                
                final_frame = pd.concat([retirees,df])
                final_frame = final_frame.pivot_table('Qty Retired','Name','Retirement Year')
                final_frame = final_frame.fillna(0)
                final_frame['Total'] = final_frame[list(final_frame.columns)].sum(axis=1)
                final_frame = final_frame.reset_index()
                final_frame = final_frame.sort_values(by=['Total'], ascending=False).reset_index(drop=True)
            else:
                pass
            results.append(final_frame)
    
        return results       
    
    
#IDs = project_list

IDs = [2000]

project = Project_Analysis(IDs)

ann_abatement = project.annual_abatement
#summary = project.df_project
balance = project.project_balance()
issuances, retirements = project.issuance_retirement_tables()
most_beneficial_owner, most_account_holder = project.top_retirees()
broker_data = project.broker_data
## ADD FUNCTION FOR PROJECT RATINGS ##
# Add something that finds low balance projects and then identifies similar projects.
# ie a corporate might have a list of similar projects they can buy and retire

z = balance.copy()
z =z.groupby(by=['Vintage']).sum()
z = z.drop(columns=['ID'])

b = broker_data.copy()
b = b.groupby(by=['Project ID','Name','Price Type']).count()['Price'].reset_index()
#b = b.pivot_table('Price','Project ID','Price Type')
b = b.pivot_table('Price','Name','Price Type')

class Retiree_Info:
    def __init__(self, owner, sub):
        self.method_types = df_projects[['Project Name','Method']]  # for merging
        
        self.retirements = df_retirement.copy()
        self.retirements = self.retirements.merge(self.method_types, how='left')       
        self.retirements['Date of Retirement'] = pd.to_datetime(self.retirements['Date of Retirement'])
        self.retirements['Year'] = [i.year for i in self.retirements['Date of Retirement']]
        self.retirements = self.retirements.dropna(subset=[sub])
        self.retirements = self.retirements[self.retirements[sub].str.contains(owner)]        
        
    def account_holders(self):
        account_holders = self.retirements.groupby(by=['Account Holder', 'Year']).sum()['Quantity of Units'].reset_index()
        account_holders = account_holders.sort_values(by=['Year','Quantity of Units'], ascending=[True,False])        
        return account_holders

    def projects(self):
        projects = self.retirements.groupby(by=['Project Name','Method']).sum()['Quantity of Units'].reset_index()
        projects = projects.sort_values(by=['Quantity of Units'], ascending=False)
        return projects
    
    def methods(self):
        methods = self.retirements.groupby(by=['Method']).sum()['Quantity of Units'].reset_index()
        methods = methods.sort_values(by=['Quantity of Units'], ascending=False)
        
        method_years = self.retirements.groupby(by=['Year','Method']).sum()['Quantity of Units'].reset_index()
        method_years = method_years.sort_values(by=['Year','Quantity of Units'], ascending=[True,False])        
        return methods, method_years
    
    def vintages(self):
        vintages = self.retirements.groupby(by=['Vintage']).sum()['Quantity of Units'].reset_index()
        vintages = vintages.sort_values(by=['Vintage'], ascending=True)
        
        vintage_years = self.retirements.groupby(by=['Year','Vintage']).sum()['Quantity of Units'].reset_index()
        vintage_years = vintage_years.sort_values(by=['Year','Vintage'])
        return vintages, vintage_years
    
owner='2234'
# Sub can be Beneficial Owner or Account Holder depending on who you want info for
retirees = Retiree_Info(owner, sub='Account Holder')

accounts = retirees.account_holders()
projects = retirees.projects()                                      
methods, methods_years = retirees.methods()
vintages, vintages_years = retirees.vintages()

retirements = retirees.retirements

#######################################################################################
## DOCUMENTATION / INFO ON A SPECIFIC PID
#######################################################################################
import sys
import importlib.util

# Specify the path to the Python file containing the class
module_path = 'C://GitHub//Voila_Testing//verra_project_analysis.py'

# Load the module from the specified path
spec = importlib.util.spec_from_file_location('Verra_Projects', module_path)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

# Import the class from the loaded module
from module_name import YourClassName

# Now you can use the imported class
instance = YourClassName()



#######################################################################################
## USING FUZZY LOOKUP FOR RETIREMENT DATA
# rename the entire retirement dataset
# is there a more efficient way to use a dict for existing matches?
# could be like an AWS db and then it scans it for existing match
#######################################################################################
## Create a fuzzy lookup function
def match_names(name, list_names, min_score=0):
    scores = pd.DataFrame()
    scores['Name'] = list_names
    ratio = []
    for i in scores.Name:
        score = fuzz.ratio(name, i)
        ratio.append(score)
    scores['Ratio'] = ratio
    scores = scores[scores.Ratio>=75]
    return scores

ret = df_retirement.copy()
ret['Year'] = [i.year for i in ret['Date of Retirement']]
ret['Month'] = [i.month for i in ret['Date of Retirement']]

retirees = ret.groupby(by=['Beneficial Owner']).sum()['Quantity of Units'].reset_index()
retirees.columns = ['Name','Qty Retired'] 


owners = list(retirees['Name'].unique())

match_dict = {}
best_name = {}

from tqdm import tqdm

for i in tqdm(owners):
    matches = match_names(i, owners)
    if len(matches) > 1:
        matches = matches.merge(retirees, on=['Name'], how="left")
        match_dict[i] = matches
        
        #year_retired = matches.groupby(by=['Retirement Year']).sum().reset_index()  
        total_retired = matches['Qty Retired'].sum()
        largest_value = max(matches['Qty Retired'])
        name = matches[matches['Qty Retired']==largest_value].reset_index(drop=True)
        name = name.Name[0]
        #year_retired['Name'] = name 
        #year_retired = year_retired[['Name','Retirement Year','Qty Retired']]
        best_name[i] = name
    else:
        best_name[i] = i        










z = df_retirement.copy()
#z = z[z['Project ID']==934]
#z = z.groupby(by=['Account Holder','Type']).sum()
z = z.groupby(by=['Beneficial Owner']).sum()
z = z.sort_values(by=['Type','Quantity of Units'], ascending=[True,False])
z = z[z['Quantity of Units']>=10000]
z = z[['Quantity of Units']].reset_index()

z.to_csv('beneficial_owner_nature_vs_renewables.csv')

#z = z.groupby(by=['Vintage']).sum()

