# -*- coding: utf-8 -*-
"""
Working on Appropriate Project Labelling
"""
from sqlalchemy import create_engine
import pandas as pd


aws_engine = create_engine('postgresql://Attunga01:875mSzNM@attunga-instance-1.c6crotlobtrk.us-east-2.rds.amazonaws.com/postgres')
engine = create_engine('postgresql://postgres:iforgot23@localhost/Voluntary_Carbon')
engine_list = [aws_engine, engine]

query = 'select * from \"VCS_Projects\"'
df_projects = pd.read_sql(query, aws_engine)
df_projects = df_projects.drop_duplicates()
df_projects['Crediting Period Start Date'] = pd.to_datetime(df_projects['Crediting Period Start Date'], format='%Y-%m-%d').dt.date
df_projects['Crediting Period End Date'] = pd.to_datetime(df_projects['Crediting Period End Date'], format='%Y-%m-%d').dt.date

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## AFOLU ##
# Blue Carbon 
afolu_arr_wrc = df_projects[(df_projects['AFOLU Activities']=='ARR; WRC') | (df_projects['AFOLU Activities']=='WRC')]
afolu_arr_wrc = afolu_arr_wrc[afolu_arr_wrc['Project ID']!=1899] # Drop sumatra
afolu_arr_wrc['Method'] = 'Blue Carbon'
projects_blue_carbon = list(afolu_arr_wrc['Project ID'].unique())

## ARR
afolu_arr = df_projects.dropna(subset=['AFOLU Activities'])
afolu_arr = afolu_arr[afolu_arr['AFOLU Activities'].str.contains('ARR')]
afolu_arr = afolu_arr[~afolu_arr['Project ID'].isin(projects_blue_carbon)]
afolu_arr['Method'] = 'ARR'
projects_arr = list(afolu_arr['Project ID'].unique())
project_mask = projects_blue_carbon + projects_arr

# IFM
afolu_ifm = df_projects.dropna(subset=['AFOLU Activities'])
afolu_ifm = afolu_ifm[afolu_ifm['AFOLU Activities'].str.contains('IFM')]
afolu_ifm = afolu_ifm[~afolu_ifm['Project ID'].isin(project_mask)]
afolu_ifm['Method'] = 'IFM'
projects_ifm = list(afolu_ifm['Project ID'].unique())
project_mask += projects_ifm

# Avoided_Def
afolu_AD = df_projects.dropna(subset=['AFOLU Activities'])
afolu_AD = afolu_AD[afolu_AD['AFOLU Activities'].str.contains('REDD')]
afolu_AD = afolu_AD[~afolu_AD['Project ID'].isin(project_mask)]
afolu_AD['Method'] = 'Avoided Def.'
projects_AD = list(afolu_AD['Project ID'].unique())
project_mask += projects_AD


# ALM
afolu_ALM = df_projects.dropna(subset=['AFOLU Activities'])
afolu_ALM = afolu_ALM[afolu_ALM['AFOLU Activities'].str.contains('ALM')]
afolu_ALM = afolu_ALM[~afolu_ALM['Project ID'].isin(project_mask)]
afolu_ALM['Method'] = 'ALM'
projects_ALM = list(afolu_ALM['Project ID'].unique())
project_mask += projects_ALM

# AFOLU Others
afolu_other = df_projects.dropna(subset=['AFOLU Activities'])
afolu_other = afolu_other[~afolu_other['Project ID'].isin(project_mask)]
afolu_other['Method'] = 'AFOLU Other'
projects_other = list(afolu_other['Project ID'].unique())
project_mask += projects_other
sub_df = df_projects[~df_projects['Project ID'].isin(project_mask)]
sub_df = sub_df[sub_df['Project Type'].str.contains('Forestry')]
sub_df['Method'] = 'AFOLU Other'
sub_projects = list(sub_df['Project ID'].unique())
project_mask += sub_projects

# Concat all AFOLU Methods
df_AFOLU = pd.concat([afolu_arr_wrc, afolu_arr, afolu_ifm, afolu_AD, afolu_ALM, afolu_other, sub_df], ignore_index=True)
df_AFOLU['Type'] = 'AFOLU'

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## NON AFOLU ##
# Cookstoves
df_cookstoves = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_cookstoves = df_cookstoves[(df_cookstoves['Project Name'].str.contains('cook', na=False, case=False)) | (df_cookstoves['Project Name'].str.contains('stove', na=False, case=False))]
df_cookstoves['Method'] = 'Cookstoves'
projects_cook = list(df_cookstoves['Project ID'].unique())
project_mask += projects_cook
df_OTHER = df_cookstoves.copy()

# Solar
df_sub = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_sub = df_sub[df_sub['Project Name'].str.contains('solar', na=False, case=False)]
df_sub['Method'] = 'Solar'
projects_sub = list(df_sub['Project ID'].unique())
project_mask += projects_sub
df_OTHER = pd.concat([df_OTHER, df_sub])

# Wind
df_sub = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_sub = df_sub[df_sub['Project Name'].str.contains('wind', na=False, case=False)]
df_sub['Method'] = 'Wind'
projects_sub = list(df_sub['Project ID'].unique())
project_mask += projects_sub
df_OTHER = pd.concat([df_OTHER, df_sub])

# Hydro
df_sub = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_sub = df_sub[df_sub['Project Name'].str.contains('hydro', na=False, case=False)]
df_sub['Method'] = 'Hydro'
projects_sub = list(df_sub['Project ID'].unique())
project_mask += projects_sub
df_OTHER = pd.concat([df_OTHER, df_sub])

# LFG
df_sub = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_sub = df_sub[(df_sub['Project Name'].str.contains('lfg', na=False, case=False)) | (df_sub['Project Name'].str.contains('fill', na=False, case=False))]
df_sub['Method'] = 'LFG'
projects_sub = list(df_sub['Project ID'].unique())
project_mask += projects_sub
df_OTHER = pd.concat([df_OTHER, df_sub])

# Waste
df_sub = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_sub = df_sub[df_sub['Project Type'].str.contains('waste', na=False, case=False)]
df_sub['Method'] = 'Waste'
projects_sub = list(df_sub['Project ID'].unique())
project_mask += projects_sub
df_OTHER = pd.concat([df_OTHER, df_sub])

# Transport
df_sub = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_sub = df_sub[df_sub['Project Type'].str.contains('transport', na=False, case=False)]
df_sub['Method'] = 'Transport'
projects_sub = list(df_sub['Project ID'].unique())
project_mask += projects_sub
df_OTHER = pd.concat([df_OTHER, df_sub])

# Construction
df_sub = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_sub = df_sub[(df_sub['Project Type'].str.contains('construction', na=False, case=False)) | (df_sub['Project Type'].str.contains('manufacturing', na=False, case=False))]
df_sub['Method'] = 'Construction / Manufacturing'
projects_sub = list(df_sub['Project ID'].unique())
project_mask += projects_sub
df_OTHER = pd.concat([df_OTHER, df_sub])

# Fugitive Emissions
df_sub = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_sub = df_sub[df_sub['Project Type'].str.contains('fugitive', na=False, case=False)]
df_sub['Method'] = 'Fugitive Emiss.'
projects_sub = list(df_sub['Project ID'].unique())
project_mask += projects_sub
df_OTHER = pd.concat([df_OTHER, df_sub])

# Livestock
df_sub = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_sub = df_sub[df_sub['Project Type'].str.contains('livestock', na=False, case=False)]
df_sub['Method'] = 'Livestock'
projects_sub = list(df_sub['Project ID'].unique())
project_mask += projects_sub
df_OTHER = pd.concat([df_OTHER, df_sub])

# Other
df_sub = df_projects[~df_projects['Project ID'].isin(project_mask)]
df_sub['Method'] = 'Other'
df_OTHER = pd.concat([df_OTHER, df_sub])

df_OTHER['Type'] = 'Non_AFOLU'

#~~~~~~~~~~~~~~~~~~~~~~~~
## MERGE 
raw = pd.concat([df_AFOLU, df_OTHER])

for e in engine_list:
    print(e)
    raw.to_sql('VCS_Projects_Labelled',e,if_exists='replace', index=False)



