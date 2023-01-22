# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 10:12:38 2023

@author: SamCurtis
"""
#######################################################################################
## LDC STUFF
#######################################################################################
app = Retrieve_Data()
## IDENTIFY NON-FORESTRY PROJECTS FROM LDCs ##
ldc_list = ['Afghanistan', 'Angola', 'Bangladesh', 'Benin', 'Bhutan', 'Burkina Faso', 'Burundi', 'Cambodia', 'Central African Republic', 'Chad', 'Comoros', 'Congo', 'Djibouti', 'Eritrea', 'Ethiopia', 'Gambia', 'Guinea', 'Guinea-Bissau', 'Haiti', 'Kiribati', 'Laos', 'Lesotho', 'Liberia', 'Madagascar', 'Malawi', 'Mali', 'Mauritania', 'Mozambique', 'Myanmar', 'Nepal', 'Niger', 'Rwanda', 'São Tomé and Príncipe', 'Senegal', 'Sierra Leone', 'Solomon Islands', 'Somalia', 'South Sudan', 'Sudan', 'Tanzania', 'Timor-Leste', 'Timor Leste', 'Togo', 'Tuvalu', 'Uganda', 'Yemen', 'Zambia']
method_mask = ['Cookstoves','Fugitive Emiss.', 'LFG']

df_projects = app.df_projects
ldc_projects = df_projects[df_projects['Country/Area'].isin(ldc_list)]

ldc_non_afolu = ldc_projects[ldc_projects.Type != 'AFOLU']
ldc_non_afolu = ldc_non_afolu.iloc[:, :-22]
ldc_desirables = ldc_non_afolu[~ldc_non_afolu.Method.isin(method_mask)]

ldc_non_afolu_ids = list(ldc_non_afolu['Project ID'].unique())
ldc_desirable_ids = list(ldc_desirables['Project ID'].unique())

ldc_desirables.to_csv('C:/GitHub/ldc_non_afolu.csv')


x = df_projects.copy()
dropcols = list(x)[15:33]
x = x.drop(columns=dropcols)

# Merge issuance and retirement data
df_issuance = app.df_issuance
df_retirement = app.df_retirement

df_issuance = df_issuance.groupby(by=['Project ID','Vintage']).sum()['Quantity of Units Issued'].reset_index()
df_retirement = df_retirement.groupby(by=['Project ID','Vintage']).sum()['Quantity of Units'].reset_index()

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

# DO LDC BALANCE BY VIN AND METHOD ##
non_afolu_iss = df_issuance[df_issuance['Project ID'].isin(ldc_non_afolu_ids)]
non_afolu_ret = df_retirement[df_retirement['Project ID'].isin(ldc_non_afolu_ids)]

non_afolu_iss = non_afolu_iss.groupby(by=['Project ID','Vintage']).sum()['Quantity of Units Issued'].reset_index()
non_afolu_ret = non_afolu_ret.groupby(by=['Project ID','Vintage']).sum()['Quantity of Units'].reset_index()
non_afolu_balance = pd.merge(non_afolu_iss, non_afolu_ret, on=['Project ID','Vintage'], how="left")
non_afolu_balance = non_afolu_balance.fillna(0)
non_afolu_balance.columns = ['Project ID', 'Vintage', 'Issued', 'Retired']
non_afolu_balance['Balance'] = non_afolu_balance.Issued - non_afolu_balance.Retired

proj_names = df_projects[['Project ID','Method','Country/Area','Project Name']]
non_afolu_balance = pd.merge(non_afolu_balance, proj_names, on=['Project ID'], how="left")
non_afolu_balance.to_csv('C:/GitHub/non_afolu_ldc_projects.csv')

#######################################################################################
#######################################################################################
# Assigning Prices to Projects
#######################################################################################

    
    
