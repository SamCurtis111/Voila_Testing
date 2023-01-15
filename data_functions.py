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

        query = 'select * from \"Verra_Issuance\"'
        self.df_issuance = pd.read_sql(query, self.engine)
        self.df_issuance = self.df_issuance.drop_duplicates()
        self.df_issuance['From Vintage'] = pd.to_datetime(self.df_issuance['From Vintage'], format='%d/%m/%Y').dt.date
        self.df_issuance['To Vintage'] = pd.to_datetime(self.df_issuance['To Vintage'], format='%d/%m/%Y').dt.date      
        self.df_issuance['Vintage'] = [i.year for i in self.df_issuance['To Vintage']]  
        
        query = 'select * from \"Verra_Retirement\"'
        self.df_retirement = pd.read_sql(query, self.engine)
        self.df_retirement = self.df_retirement.drop_duplicates()
        self.df_retirement['Date of Retirement'] = pd.to_datetime(self.df_retirement['Date of Retirement'], format='%Y-%m-%d').dt.date
        self.df_retirement['From Vintage'] = pd.to_datetime(self.df_retirement['From Vintage'], format='%Y-%m-%d').dt.date
        self.df_retirement['To Vintage'] = pd.to_datetime(self.df_retirement['To Vintage'], format='%Y-%m-%d').dt.date      
        self.df_retirement['Vintage'] = [i.year for i in self.df_retirement['To Vintage']] 

        query = 'select * from \"VCS_Projects\"'
        self.df_projects = pd.read_sql(query, self.engine)
        self.df_projects = self.df_projects.drop_duplicates()
        self.df_projects['Crediting Period Start Date'] = pd.to_datetime(self.df_projects['Crediting Period Start Date'], format='%Y-%m-%d').dt.date
        self.df_projects['Crediting Period End Date'] = pd.to_datetime(self.df_projects['Crediting Period End Date'], format='%Y-%m-%d').dt.date

        self.ngeo_issuance, self.ngeo_retirement = self.ngeo_eligibility()
        self.assign_methods()
        
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

    def assign_methods(self):
        # COOKSTOVES #
        cookstrings = ['Stove','stove','Cooking','cooking','Cook','cook', 'STOVE','COOK', 'stew']
        df_stove = self.df_issuance[self.df_issuance['Project Name'].str.contains('Stove')]
        for c in cookstrings[1:]:
            df_sub = self.df_issuance[self.df_issuance['Project Name'].str.contains(c)]
            df_stove = pd.concat([df_stove, df_sub])
        df_stove = df_stove.drop_duplicates()
        df_stove['Method'] = 'cookstoves'
        
        # SOLAR #
        solarstrings = ['Solar','solar','Photo','photo','PV', 'SOLAR', 'Pv']
        df_solar = self.df_issuance[self.df_issuance['Project Name'].str.contains('Solar')]
        for s in solarstrings[1:]:
            df_sub = self.df_issuance[self.df_issuance['Project Name'].str.contains(s)]
            df_solar = pd.concat([df_solar, df_sub])
        df_solar = df_solar.drop_duplicates()
        df_solar['Method'] = 'solar'
        
        # HYDRO #
        strings = ['Hydro','hydro','River','river','HEPP', 'HYDRO', 'Foz do', 'Pizarras', 'Fundao', 'Hyrdro', 'Low Dam']
        df_hydro = self.df_issuance[self.df_issuance['Project Name'].str.contains('Hydro')]
        for s in strings[1:]:
            df_sub = self.df_issuance[self.df_issuance['Project Name'].str.contains(s)]
            df_hydro = pd.concat([df_hydro, df_sub])
        df_hydro = df_hydro.drop_duplicates()
        df_hydro['Method'] = 'hydro'
        
        # WIND #
        strings = ['Wind','wind', 'WIND']
        df_wind = self.df_issuance[self.df_issuance['Project Name'].str.contains('Wind')]
        for s in strings[1:]:
            df_sub = self.df_issuance[self.df_issuance['Project Name'].str.contains(s)]
            df_wind = pd.concat([df_wind, df_sub])
        df_wind = df_wind.drop_duplicates()
        df_wind['Method'] = 'wind'
        
        # LANDFILL GAS / WASTE / Fugitive Emissions / CCS #
        strings = ['Fill','fill', 'Waste','waste','CMM','CCS','capture','Capture', 'Biogas','biogas', 'LFG', 'Methane','methane','METHANE','Gas','gas', 'Biomass','biomass', 'compost', 'LNG', 'LANDFILL', 'Compost', 'Composting']
        df_lfg = self.df_issuance[self.df_issuance['Project Type'].str.contains('Fugitive')]
        for s in strings:
            df_sub = self.df_issuance[self.df_issuance['Project Name'].str.contains(s)]
            df_lfg = pd.concat([df_lfg, df_sub])
        df_lfg = df_lfg[~df_lfg['Project Type'].str.contains('Livestock')]
        df_lfg = df_lfg[~df_lfg['Project Type'].str.contains('Forest')]
        df_lfg = df_lfg.drop_duplicates()
        df_lfg['Method'] = 'lfg_ccs_gas_biomass'
        
        # GEOTHERMAL / BIOMASS #
        strings = ['Geothermal','geothermal', 'Thermal','thermal']
        df_geothermal = self.df_issuance[self.df_issuance['Project Name'].str.contains('Geothermal')]
        for s in strings[1:]:
            df_sub = self.df_issuance[self.df_issuance['Project Name'].str.contains(s)]
            df_geothermal = pd.concat([df_geothermal, df_sub])
        df_geothermal = df_geothermal.drop_duplicates()
        df_geothermal['Method'] = 'geothermal'
        
        # LIVESTOCK / METHANE #
        strings = ['Methane','methane','Dairy','dairy']
        df_livestock = self.df_issuance[self.df_issuance['Project Name'].str.contains('Methane')]
        for s in strings[1:]:
            df_sub = self.df_issuance[self.df_issuance['Project Name'].str.contains(s)]
            df_livestock = pd.concat([df_livestock, df_sub])
        df_livestock = df_livestock[df_livestock['Project Type'].str.contains('Livestock')]    
        df_livestock = df_livestock.drop_duplicates()
        df_livestock['Method'] = 'livestock_methane'
        
        # FUEL SWITCHING #
        strings = ['Switching','SWITCHING','switching', 'Husk', 'husk', 'Smelter', 'smelter', 'Switch']
        df_switching = self.df_issuance[self.df_issuance['Project Name'].str.contains('Switching')]
        for s in strings[1:]:
            df_sub = self.df_issuance[self.df_issuance['Project Name'].str.contains(s)]
            df_switching = pd.concat([df_switching, df_sub])  
        df_switching = df_switching.drop_duplicates()
        df_switching['Method'] = 'fuel_switching'
        
        ## AFFORESTATION ##
        strings = ['Afforestation','afforestation','Plantation','plantation']
        arr_projects = self.df_projects[self.df_projects['AFOLU Activities']=='ARR']
        arr_projects = list(arr_projects['Project ID'].unique())
        
        df_arr = self.df_issuance[self.df_issuance['Project Name'].str.contains('Afforestation')]
        for s in strings[1:]:
            df_sub = self.df_issuance[self.df_issuance['Project Name'].str.contains(s)]
            df_arr = pd.concat([df_arr, df_sub])
        x = self.df_issuance[self.df_issuance['Project ID'].isin(arr_projects)]
        df_arr = pd.concat([df_arr, x])
        df_arr = df_arr.drop_duplicates()
        df_arr['Method'] = 'arr'
        
        ## AVOIDED DEFORESTATION ##
        df_avoided = self.df_issuance[self.df_issuance['Project Type'].str.contains('Forest')]
        affo_projects = list(df_arr['Project ID'].unique())
        df_avoided = df_avoided[~df_avoided['Project ID'].isin(affo_projects)]
        df_avoided['Method'] = 'forestry_avoided'
        df_avoided = df_avoided.drop_duplicates()
        
        # Chemical #
        df_chemical = self.df_issuance[self.df_issuance['Project Type'].str.contains('Chemical')]
        df_chemical['Method'] = 'chemicals'
        df_chemical = df_chemical.drop_duplicates()
        
        # Plastic #
        df_plastic = self.df_issuance[self.df_issuance['Project Type'].str.contains('Plastic')]
        df_plastic['Method'] = 'plastics'
        df_plastic = df_plastic.drop_duplicates()
        
        # Transport #
        df_transport = self.df_issuance[self.df_issuance['Project Type'].str.contains('Transport')]
        df_transport['Method'] = 'transport'
        df_transport = df_transport.drop_duplicates()
        
        # BLUE CARBON #
        sub1 = self.df_issuance[self.df_issuance['Project Name'].str.contains('BLUE')]
        sub2 = self.df_issuance[self.df_issuance['Project Name'].str.contains('Blue')]
        df_blue = pd.concat([sub1, sub2])
        df_blue['Method'] = 'Blue Carbon'
        df_blue = df_blue.drop_duplicates()
        
        
        ## MERGE THEM ALL TOGETHER ##
        df_issuance_merged = pd.concat([df_blue, df_stove, df_solar, df_hydro, df_wind, df_lfg, df_livestock, df_arr, df_avoided, df_chemical, df_plastic, df_transport, df_geothermal, df_switching])
        
        # Other non-forestry # IDENTIFY THE MISSING PROJECTS
        stripped_projects = list(df_issuance_merged['Project ID'].unique())
        missing_projects = self.df_issuance[~self.df_issuance['Project ID'].isin(stripped_projects)] # find the projects that aren't yet accounted for
        missing_projects['Method'] = 'other_non_forestry'
        
        df_issuance_merged = pd.concat([df_issuance_merged, missing_projects])
        df_issuance_merged = df_issuance_merged.drop_duplicates(subset=list(df_issuance_merged)[:-1])
        df_issuance_merged = df_issuance_merged.sort_values(by=['Issuance Date','Project ID','To Vintage'], ascending=[False, True, True])
        self.df_issuance = df_issuance_merged.copy()
        
        ## UPDATE THE RETIREMENT DATA TO INCLUDE METHODS ##
        sub_df = self.df_issuance[['Project ID','Method']]
        df_retirement = self.df_retirement.merge(sub_df, on='Project ID')
        df_retirement = df_retirement.drop_duplicates().reset_index(drop=True)
        self.df_retirement = df_retirement.copy()
        
        ## UPDATE THE PROJECTS DATA TO INCLUDE METHODS ##
        sub_df = self.df_issuance[['Project ID','Method']]
        df_project = self.df_projects.merge(sub_df, on='Project ID')
        df_project = df_project.drop_duplicates().reset_index(drop=True)
        self.df_projects = df_project.copy()
        
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
            vintage_retirement = df.groupby(by=['Retirement_Month']).mean()['Vintage'].reset_index()
            qty_retirement = df.groupby(by=['Retirement_Month']).sum()['Quantity of Units'].reset_index()
            
            df = vintage_retirement.merge(qty_retirement, on='Retirement_Month')
            df['Vintage'] = round(df.Vintage, 0).astype(int)
            df.columns = ['Year_Mth', 'Vintage', 'Quantity']
            return df

    

app = Retrieve_Data()

for e in engine_list:
    app.vintage_retirements().to_sql('Retirements_Monthly_Vintage', e, if_exists='replace', index=False)
    app.unit_balance().reset_index().to_sql('Vintage_Balances', e, if_exists='replace', index=False)
    app.unit_balance(merge_group='NGEO').reset_index().to_sql('Vintage_Balances_NGEO', e, if_exists='replace', index=False)
    app.df_issuance.to_sql('VCS_Issuance_Labeled', e, if_exists='replace', index=False)
    app.df_retirement.to_sql('VCS_Retirement_Labeled', e, if_exists='replace', index=False)
    app.ngeo_issuance.to_sql('NGEO_Issuance', e, if_exists='replace', index=False)
    app.ngeo_retirement.to_sql('NGEO_Retirement', e, if_exists='replace', index=False)
