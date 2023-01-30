# -*- coding: utf-8 -*-
"""
Functions
"""
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
engine = create_engine('postgresql://postgres:iforgot23@localhost/Voluntary_Carbon')
aws_engine = create_engine('postgresql://Attunga01:875mSzNM@attunga-instance-1.c6crotlobtrk.us-east-2.rds.amazonaws.com/postgres')
engine_list = [engine, aws_engine]


class Retrieve_Data:
    def __init__(self):
        self.engine = create_engine('postgresql://Attunga01:875mSzNM@attunga-instance-1.c6crotlobtrk.us-east-2.rds.amazonaws.com/postgres')

        query = 'select * from \"VCS_Projects_Labelled\"'
        self.df_projects = pd.read_sql(query, self.engine)
        self.df_projects = self.df_projects.drop_duplicates()
        self.df_projects['Crediting Period Start Date'] = pd.to_datetime(self.df_projects['Crediting Period Start Date'], format='%Y-%m-%d').dt.date
        self.df_projects['Crediting Period End Date'] = pd.to_datetime(self.df_projects['Crediting Period End Date'], format='%Y-%m-%d').dt.date

        self.project_merge = self.df_projects.copy()
        self.project_merge = self.project_merge[['Project ID','Method','Type']]

        query = 'select * from \"Verra_Issuance\"'
        self.df_issuance = pd.read_sql(query, self.engine)
        self.df_issuance = self.df_issuance.drop_duplicates()
        self.df_issuance['From Vintage'] = pd.to_datetime(self.df_issuance['From Vintage'], format='%d/%m/%Y').dt.date
        self.df_issuance['To Vintage'] = pd.to_datetime(self.df_issuance['To Vintage'], format='%d/%m/%Y').dt.date      
        self.df_issuance['Vintage'] = [i.year for i in self.df_issuance['To Vintage']]  
        self.df_issuance = pd.merge(self.df_issuance, self.project_merge, on='Project ID', how='left')
        
        query = 'select * from \"Verra_Retirement\"'
        self.df_retirement = pd.read_sql(query, self.engine)
        self.df_retirement = self.df_retirement.drop_duplicates()
        self.df_retirement['Date of Retirement'] = pd.to_datetime(self.df_retirement['Date of Retirement'], format='%Y-%m-%d').dt.date
        self.df_retirement['From Vintage'] = pd.to_datetime(self.df_retirement['From Vintage'], format='%Y-%m-%d').dt.date
        self.df_retirement['To Vintage'] = pd.to_datetime(self.df_retirement['To Vintage'], format='%Y-%m-%d').dt.date      
        self.df_retirement['Vintage'] = [i.year for i in self.df_retirement['To Vintage']] 
        self.df_retirement = pd.merge(self.df_retirement, self.project_merge, on='Project ID', how='left')

        query = 'select * from \"Broker_Markets\"'
        self.df_broker = pd.read_sql(query, self.engine)
        
        query = 'select * from \"SIP_Settles\"'
        self.df_sip_settles = pd.read_sql(query, self.engine)        
        

        self.ngeo_issuance, self.ngeo_retirement = self.ngeo_eligibility()
        
        self.ngeo_project_list = list(self.ngeo_issuance['Project ID'].unique())
        self.ngeo_project_list_string = ['VCS ' + str(i) for i in self.ngeo_project_list]
        
        self.ngeo_undesirable_list = self.determine_undesirable_ngeos()
        
        self.ldc_list = ['Afghanistan', 'Angola', 'Bangladesh', 'Benin', 'Bhutan', 'Burkina Faso', 'Burundi', 'Cambodia', 'Central African Republic', 'Chad', 'Comoros', 'Congo', 'Djibouti', 'Eritrea', 'Ethiopia', 'Gambia', 'Guinea', 'Guinea-Bissau', 'Haiti', 'Kiribati', 'Laos', 'Lesotho', 'Liberia', 'Madagascar', 'Malawi', 'Mali', 'Mauritania', 'Mozambique', 'Myanmar', 'Nepal', 'Niger', 'Rwanda', 'São Tomé and Príncipe', 'Senegal', 'Sierra Leone', 'Solomon Islands', 'Somalia', 'South Sudan', 'Sudan', 'Tanzania', 'Timor-Leste', 'Timor Leste', 'Togo', 'Tuvalu', 'Uganda', 'Yemen', 'Zambia']
        
        self.today = datetime.today()
        self.yesterday = self.today - timedelta(days=1)
        self.yesterday = self.yesterday.date()
        
    def ngeo_eligibility(self):
            #---------------------
            # One Hot Encode the certifications
            certification_frame = (self.df_projects['Additional Issuance Certifications'].str.split(r's*,s*', expand=True)
               .apply(pd.Series.value_counts, 1)
               .iloc[:, 1:]
               .fillna(0, downcast='infer'))
            
            #certification_frame = df_projects.copy()
            #cols = list(certification_frame)
            # Add grouping columns (e.g. all SDG and all CCD)
            #sdg_cols = [col for col in certification_frame.columns if ':' in col]  # uncomment this to figure out GEO projects later
            #ccb_cols = []
            #for c in cols:
             #   if 'CCB' in c:
              #      ccb_cols.append(c)
                    
            #ccb_cols.insert(0, 'Project ID')        
                    
            #sub_df = certification_frame.copy()        
            #sub_df = sub_df[ccb_cols]
            #sub_df = sub_df.set_index('Project ID')
            #sub_df['CCB_Any'] = sub_df.sum(axis=1)
            #sub_df = sub_df[sub_df.CCB_Any > 0].reset_index()
            #ngeo_projects = list(sub_df['Project ID'].unique())
            
            # Add grouping columns (e.g. all SDG and all CCD)
            sdg_cols = [col for col in certification_frame.columns if ':' in col]
            ccb_cols = [col for col in certification_frame.columns if 'CCB-' in col]
            
            certification_frame['SDG'] = np.where((certification_frame[sdg_cols]==1).any(axis=1),1,0)
            certification_frame['CCB'] = np.where((certification_frame[ccb_cols]==1).any(axis=1),1,0)
            
            try:
                any_cert_cols = ['SDG','CCB','CORSIA','Social Carbon']
                certification_frame['No Additional Cert'] = np.where((certification_frame[any_cert_cols]==0).all(axis=1),1,0)
            except KeyError:
                any_cert_cols = ['SDG','CCB']#,'Social Carbon']
                certification_frame['No Additional Cert'] = np.where((certification_frame[any_cert_cols]==0).all(axis=1),1,0)
            
            self.df_projects = pd.concat([self.df_projects, certification_frame], axis=1)
            self.df_projects = self.df_projects.drop(columns=['Additional Issuance Certifications'])
            #------------------------
            #------------------------
            # Determine NGEO Eligible Projects
            ngeo_projects = self.df_projects[(self.df_projects.CCB==1)]
            ngeo_projects = ngeo_projects.drop_duplicates(subset='Project ID')
            
            ngeo_projects = list(ngeo_projects['Project ID'].unique())
            #--------------------------
            #--------------------------
            # Get supply / demand of NGEO eligible projects
            ngeo_issuance = self.df_issuance[self.df_issuance['Project ID'].isin(ngeo_projects)].drop_duplicates()
            ngeo_retirement = self.df_retirement[self.df_retirement['Project ID'].isin(ngeo_projects)].drop_duplicates()
            return ngeo_issuance, ngeo_retirement    


        
    ##############################################################
    # DATA MODELLING / ANALYSIS
    ##############################################################
    def unit_balance(self, merge_group='All'):
        if merge_group=='All':
            grouped_issuance = self.df_issuance.copy()
            grouped_retirement = self.df_retirement.copy()
            grouped_issuance = grouped_issuance.groupby(by=['Vintage','Method']).sum()['Quantity of Units Issued'].reset_index()
            grouped_retirement = grouped_retirement.groupby(by=['Vintage','Method']).sum()['Quantity of Units'].reset_index()
            method_balance = grouped_issuance.merge(grouped_retirement, how='left',on=['Vintage','Method'])
        elif merge_group=='NGEO':
            grouped_issuance = self.ngeo_issuance.groupby(by='Vintage').sum()['Quantity of Units Issued'].reset_index()
            grouped_retirement = self.ngeo_retirement.groupby(by='Vintage').sum()['Quantity of Units'].reset_index()
            method_balance = grouped_issuance.merge(grouped_retirement, how='left',on='Vintage')
        else:
            grouped_issuance=self.df_issuance[self.df_issuance['Method']==merge_group].reset_index(drop=True)
            grouped_retirement = self.df_retirement[self.df_retirement['Method']==merge_group].reset_index(drop=True)
            grouped_issuance = grouped_issuance.groupby(by='Vintage').sum()['Quantity of Units Issued'].reset_index()
            grouped_retirement = grouped_retirement.groupby(by='Vintage').sum()['Quantity of Units'].reset_index()
        
        
        method_balance['Remaining'] = method_balance['Quantity of Units Issued'] - method_balance['Quantity of Units']
        
        method_balance = method_balance.set_index('Vintage')
        return method_balance    
    
    ##############
    # CREDIT RETIREMENT ANALYSIS
    def vintage_retirements(self):
            df = self.df_retirement.copy()
            df['Date of Retirement'] = pd.to_datetime(df['Date of Retirement'])
            df['Retirement_Month'] = [str(i.year)[2:]+'_'+str(i.month).zfill(2) for i in df['Date of Retirement']]

            x = df.groupby(by=['Retirement_Month','Vintage']).sum()['Quantity of Units'].reset_index()
            x['Vin_Product'] = x.Vintage * x['Quantity of Units']
            
            z = x.groupby(by='Retirement_Month').sum().reset_index()
            z['Average_Vin'] = z.Vin_Product / z['Quantity of Units']
            z['Average_Vin'] = round(z['Average_Vin'],0)            

            z.columns = ['Year_Mth', 'Vins', 'Quantity','Product', 'Vintage']
            z = z.drop(columns=['Vins','Product'])
            return z
        
    #########
    # Issuance : Retirement Ratios by Method
    def retirement_ratios(self):
        z=self.df_issuance
        z = z.drop_duplicates()
        z = z.sort_values(by='Issuance Date').reset_index(drop=True)
        z['YY_MM'] = [str(i.year)[2:]+'_'+str(i.month).zfill(2) for i in z['Issuance Date']]
        zz = z.groupby(by=['Method','YY_MM']).sum()['Quantity of Units Issued'].reset_index()
        
                
        x = self.df_retirement
        x = x.drop_duplicates()
        x = x.sort_values(by='Date of Retirement').reset_index(drop=True)
        x['YY_MM'] = [str(i.year)[2:]+'_'+str(i.month).zfill(2) for i in x['Date of Retirement']]
        xx = x.groupby(by=['Method','YY_MM']).sum()['Quantity of Units'].reset_index()
                
        z_dates = zz[['Method', 'YY_MM']]
        x_dates = xx[['Method', 'YY_MM']]
        all_dates = pd.concat([z_dates, x_dates])
        all_dates = all_dates.drop_duplicates()
        all_dates = all_dates.sort_values(by=['Method','YY_MM'])
                
        raw = all_dates.copy()
        raw = pd.merge(all_dates, zz, on=['Method','YY_MM'], how='left')
        raw = pd.merge(raw, xx, on=['Method','YY_MM'], how='left')
        raw['Rolling_Issuance'] = raw.groupby('Method')['Quantity of Units Issued'].transform(lambda i: i.expanding().sum())
        raw['Rolling_Retirement'] = raw.groupby('Method')['Quantity of Units'].transform(lambda i: i.expanding().sum())
        raw = raw.fillna(0)
        raw['Balance'] = raw.Rolling_Issuance - raw.Rolling_Retirement
        raw['Retirement_Ratio'] = raw.Rolling_Retirement / raw.Rolling_Issuance
        return raw
    
    def ldc_projects(self):
        df_ldc = self.df_projects[self.df_projects['Country/Area'].isin(self.ldc_list)]
        dropcols = list(df_ldc)[15:33]
        df_ldc = df_ldc.drop(columns=dropcols)
        return df_ldc
    
    def ldc_project_balances(self):
        # Merge issuance and retirement data
        df_issuance = self.df_issuance
        df_retirement = self.df_retirement
        df_projects = app.ldc_projects()
        ldc_ids = list(df_projects['Project ID'].unique())
        
        df_issuance = df_issuance[df_issuance['Project ID'].isin(ldc_ids)]
        df_retirement = df_retirement[df_retirement['Project ID'].isin(ldc_ids)]
        
        df_issuance = df_issuance.groupby(by=['Project ID','Vintage']).sum()['Quantity of Units Issued'].reset_index()
        df_retirement = df_retirement.groupby(by=['Project ID','Vintage']).sum()['Quantity of Units'].reset_index()        
        
        df_balance = pd.merge(df_issuance, df_retirement, on=['Project ID','Vintage'], how="left")
        df_balance = df_balance.fillna(0)
        df_balance.columns = ['Project ID','Vintage','Issued','Retired']
        df_balance['Balance'] = df_balance.Issued - df_balance.Retired
        
        # Add the project names
        proj_names = df_projects[['Project ID','Method','Type','Country/Area','Project Name','Status','Estimated Annual Emission Reductions']]
        balances = pd.merge(df_balance, proj_names, on=['Project ID'], how="left")
        return balances
    
    ## GET THE VINTAGE BALANCES OF NGEO ELIGIBLE PROJECTS
    def ngeo_project_balances(self):
        issuance = self.ngeo_issuance.copy()
        retirement = self.ngeo_retirement.copy()
        
        issuance = issuance.groupby(by=['Vintage','Project ID','Method','Project Name','Project Country/Area']).sum()['Quantity of Units Issued'].reset_index()
        issuance.columns = ['Vintage','ID','Method','Name','Country','Issued']     
        retirement = retirement.groupby(by=['Vintage','Project ID','Method','Project Name','Project Country/Area']).sum()['Quantity of Units'].reset_index()
        retirement.columns = ['Vintage','ID','Method','Name','Country','Retired']
        
        balance = pd.merge(issuance, retirement, on=['Vintage','ID','Method','Name','Country'], how="left")
        balance = balance.fillna(0)
        balance['Balance'] = balance.Issued - balance.Retired
        return balance
    
    # Yesterday issuances and retirements
    def yesterday_issuance_retirement(self):
        yest_issuances = self.df_issuance[self.df_issuance['Issuance Date']==self.yesterday]
        yest_issuances = yest_issuances[['Issuance Date','Project ID','Project Name','Project Country/Area','Method','Vintage','Quantity of Units Issued','Vintage Report Total']]
        yest_issuances.columns = ['Date','ID','Name','Country','Method','Vintage','Units Issued','Issued Per Vintage']
        
        yest_retirement = self.df_retirement[self.df_retirement['Date of Retirement']==self.yesterday]
        yest_retirement = yest_retirement[['Date of Retirement','Project ID','Project Name','Project Country/Area','Method','Vintage','Quantity of Units','Account Holder','Beneficial Owner','Retirement Reason Details']]
        yest_retirement.columns = ['Date','ID','Name','Country','Method','Vintage','Qty','Account Holder','Beneficial Owner','Reason']
        
        return yest_issuances, yest_retirement
    
    # Any NGEO project thats not bid / traded in past 2 yrs
    def determine_undesirable_ngeos(self):
        df = self.df_broker[(self.df_broker['Price Type']=="Bid") | (self.df_broker['Price Type']=="Trade")]
        df = df[df.Year>=2022].reset_index(drop=True)
        df = df[df['Project ID'].isin(self.ngeo_project_list_string)]   
        desirables = list(df['Project ID'].unique())
        desirables = [int(i[4:]) for i in desirables]
        
        undesirables = self.ngeo_issuance[~(self.ngeo_issuance['Project ID'].isin(desirables))].copy()
        undesriables = list(undesirables['Project ID'].unique())
        return undesriables
    
    # Analysis on 'undesirable' NGEO projects (those that haven't been bid/traded in past 2 years)
    def ngeo_undesirable_vintage_balances(self):
        issued = self.df_issuance[self.df_issuance['Project ID'].isin(self.ngeo_undesirable_list)].copy()
        issued = issued.groupby(by=['Project ID', 'Vintage']).sum()['Quantity of Units Issued'].reset_index()
        retired = self.df_retirement[self.df_retirement['Project ID'].isin(self.ngeo_undesirable_list)].copy()
        retired = retired.groupby(by=['Project ID', 'Vintage']).sum()['Quantity of Units'].reset_index()
        balance = pd.merge(issued, retired, on=['Project ID', 'Vintage'], how="left")
        balance = balance.fillna(0)
        balance.columns = ['ID', 'Vintage', 'Issued', 'Retired']
        balance['Balance'] = balance.Issued - balance.Retired
        #balance = balance[balance.Vintage >= 2016]
        balance_grouped = balance.groupby(by=['Vintage']).sum().reset_index()
        balance_grouped = balance_grouped.drop(columns='ID')
        return balance, balance_grouped

    def ngeo_undesirable_project_balances(self):
        retirement = self.ngeo_retirement.copy()
        retirement['YY'] = [i.year for i in retirement['Date of Retirement']]
        retirement = retirement[retirement['Project ID'].isin(self.ngeo_undesirable_list)]
        retirement = retirement.groupby(by=['Project ID','Vintage','YY']).sum()['Quantity of Units'].reset_index()
        
        issuance = self.ngeo_issuance.copy()
        issuance['YY'] = [i.year for i in issuance['Issuance Date']]
        issuance = issuance[issuance['Project ID'].isin(self.ngeo_undesirable_list)]
        issuance = issuance.groupby(by=['Project ID','Vintage','YY']).sum()['Quantity of Units Issued'].reset_index()
        
        undesirable_balances = pd.merge(issuance, retirement, on=['Project ID','Vintage','YY'], how="left")
        undesirable_balances = undesirable_balances.fillna(0)
        undesirable_balances.columns = ['Project ID','Vintage','Retirement Year','Issued','Retired']
        return undesirable_balances
        
        

app = Retrieve_Data()

for e in engine_list:
    app.ldc_projects().to_sql('LDC_Projects', e, if_exists='replace', index=False)
    app.ldc_project_balances().to_sql('LDC_Project_Balances', e, if_exists='replace', index=False)
    app.vintage_retirements().to_sql('Retirements_Monthly_Vintage', e, if_exists='replace', index=False)
    app.unit_balance().reset_index().to_sql('Vintage_Balances', e, if_exists='replace', index=False)
    app.unit_balance(merge_group='NGEO').reset_index().to_sql('Vintage_Balances_NGEO', e, if_exists='replace', index=False)
    app.df_issuance.to_sql('VCS_Issuance_Labeled', e, if_exists='replace', index=False)
    app.df_retirement.to_sql('VCS_Retirement_Labeled', e, if_exists='replace', index=False)
    app.ngeo_issuance.to_sql('NGEO_Issuance', e, if_exists='replace', index=False)
    app.ngeo_retirement.to_sql('NGEO_Retirement', e, if_exists='replace', index=False)
    app.retirement_ratios().to_sql('Method_Retirement_Ratios', e, if_exists='replace', index=False)
    app.ngeo_project_balances().to_sql('NGEO_Projects_Vintage_Balances', e, if_exists='replace', index=False)
    iss, ret = app.yesterday_issuance_retirement()
    iss.to_sql('VCS_Overnight_Issuance', e, if_exists='replace', index=False)
    ret.to_sql('VCS_Overnight_Retirement', e, if_exists='replace', index=False)
    undesirable_ngeo_projet_balances, undesirable_ngeo_vintage_balances = app.ngeo_undesirable_vintage_balances()
    undesirable_ngeo_projet_balances.to_sql('NGEO_Undesirable_Projects', e, if_exists='replace', index=False)
    undesirable_ngeo_vintage_balances.to_sql('NGEO_Undesirable_Vintages', e, if_exists='replace', index=False)
    app.ngeo_undesirable_project_balances().to_sql('NGEO_Undesirable_Projects_Dated', e, if_exists='replace', index=False)
