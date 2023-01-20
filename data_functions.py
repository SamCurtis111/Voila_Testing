# -*- coding: utf-8 -*-
"""
Functions
"""
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
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

        self.ngeo_issuance, self.ngeo_retirement = self.ngeo_eligibility()
        self.ldc_list = ['Afghanistan', 'Angola', 'Bangladesh', 'Benin', 'Bhutan', 'Burkina Faso', 'Burundi', 'Cambodia', 'Central African Republic', 'Chad', 'Comoros', 'Congo', 'Djibouti', 'Eritrea', 'Ethiopia', 'Gambia', 'Guinea', 'Guinea-Bissau', 'Haiti', 'Kiribati', 'Laos', 'Lesotho', 'Liberia', 'Madagascar', 'Malawi', 'Mali', 'Mauritania', 'Mozambique', 'Myanmar', 'Nepal', 'Niger', 'Rwanda', 'São Tomé and Príncipe', 'Senegal', 'Sierra Leone', 'Solomon Islands', 'Somalia', 'South Sudan', 'Sudan', 'Tanzania', 'Timor-Leste', 'Timor Leste', 'Togo', 'Tuvalu', 'Uganda', 'Yemen', 'Zambia']
        
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
        df_ldc = self.df_projects[self.df_projects.isin(self.ldc_list)]
        dropcols = list(df_ldc)[15:33]
        df_ldc = df_ldc.drop(columns=dropcols)
        return df_ldc


app = Retrieve_Data()

for e in engine_list:
    app.ldc_projects().to_sql('LDC_Projects', e, if_exists='replace', index=False)
    app.vintage_retirements().to_sql('Retirements_Monthly_Vintage', e, if_exists='replace', index=False)
    app.unit_balance().reset_index().to_sql('Vintage_Balances', e, if_exists='replace', index=False)
    app.unit_balance(merge_group='NGEO').reset_index().to_sql('Vintage_Balances_NGEO', e, if_exists='replace', index=False)
    app.df_issuance.to_sql('VCS_Issuance_Labeled', e, if_exists='replace', index=False)
    app.df_retirement.to_sql('VCS_Retirement_Labeled', e, if_exists='replace', index=False)
    app.ngeo_issuance.to_sql('NGEO_Issuance', e, if_exists='replace', index=False)
    app.ngeo_retirement.to_sql('NGEO_Retirement', e, if_exists='replace', index=False)
    app.retirement_ratios().to_sql('Method_Retirement_Ratios', e, if_exists='replace', index=False)



