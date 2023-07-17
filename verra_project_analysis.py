# -*- coding: utf-8 -*-
"""
VERRA SCRAPE

Class returns all information on a project

SDG function checks if there are any sdg documents in the project documentation
"""
import requests
#from urllib.request import request
from urllib import request
from tqdm import tqdm


ID = 1742

## DONT NEED A YIELD CURVE ON RENEWABLES ##

class Verra_Projects:
    def __init__(self, ID):
        self.api_url = "https://registry.verra.org/uiapi/resource/resourceSummary/{}".format(str(ID))
        self.data = requests.get(self.api_url).json()
        self.html = request.urlopen(self.api_url).read()
        
        self.heading = self.data['resourceName']
        self.ID = self.data['resourceIdentifier']
        self.i = self.data['participationSummaries'][0]
        #print(self.i['attributes'])
        self.Category = self.i['programCode']
        self.documents = self.data['documentGroups']
        
        self.attributes = self.attribute_check()
        self.annual_emissions = int(self.attributes['EST_ANNUAL_EMISSION_REDCT'])
        
    def SDG_check(self):
        substring = ['_SD_','SD_','SDG']
        match = []
        for d in self.documents:
            doc = d['documents']
            for i in doc:
                docname = i['documentName']
                for sub in substring:
                    if sub in docname:
                        match.append('True')
        return match
        
    def attribute_check(self):
        attribute_dict = {}
        for j in self.i['attributes']:
            info = j['code']
            attribute_dict[info] = j['values'][0]['value']
        return attribute_dict

# registered_projects comes from the CORSIA section of scratchpad.py
projects = registered_projects
#projects2 = projects2[1:] # from where it last stopped

total_supply = []
IDs = []
for t in tqdm(projects):
    try:
        project = Verra_Projects(t)
    except KeyError:
        pass
    check = project.SDG_check()    
    if 'True' in check:
        print(str(t))
        total_supply.append(project.annual_emissions)
        IDs.append(project.ID)

# If the above manages to finish, write the results into a new DB and set up automation
        
df_registered_vcs_sdg_supply = pd.DataFrame()
df_registered_vcs_sdg_supply['ID'] = IDs
df_registered_vcs_sdg_supply['Annual_Credits'] = total_supply
df_registered_vcs_sdg_supply.to_csv('VCS_SDG_Registered.csv')


## some of the functionality: ##
project = Verra_Projects(ID)
name = project.heading
category = project.Category   
atts = project.attributes            
check = project.SDG_check()
documents = project.documents   
project.annual_emissions

