# -*- coding: utf-8 -*-
"""
Working on Appropriate Project Labelling
"""
from sqlalchemy import create_engine
import pandas as pd


aws_engine = create_engine('postgresql://Attunga01:875mSzNM@attunga-instance-1.c6crotlobtrk.us-east-2.rds.amazonaws.com/postgres')
engine = create_engine('postgresql://postgres:iforgot23@localhost/Voluntary_Carbon')
engine_list = [engine, aws_engine]

query = 'select * from \"VCS_Projects\"'
df_projects = pd.read_sql(query, aws_engine)
df_projects = df_projects.drop_duplicates()
df_projects['Crediting Period Start Date'] = pd.to_datetime(df_projects['Crediting Period Start Date'], format='%Y-%m-%d').dt.date
df_projects['Crediting Period End Date'] = pd.to_datetime(df_projects['Crediting Period End Date'], format='%Y-%m-%d').dt.date

#~~~~~~~~~~~~
# FIX AFOLU LABELLING
df_afolu = df_projects.copy()
df_afolu = df_afolu[~df_afolu['AFOLU Activities'].isna()]
df_afolu['Method'] = df_afolu['AFOLU Activities']
df_afolu['Type'] = 'AFOLU'

project_mask = list(df_afolu['Project ID'].unique())


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
raw = pd.concat([df_afolu, df_OTHER])

for e in engine_list:
    print(e)
    raw.to_sql('VCS_Projects_Labelled',e,if_exists='replace', index=False)



