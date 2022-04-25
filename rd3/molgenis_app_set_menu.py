#'////////////////////////////////////////////////////////////////////////////
#' FILE: molgenis_app_set_menu.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-01-18
#' MODIFIED: 2022-04-25
#' PURPOSE: Update the Menu setting in Application Settings
#' STATUS: stable
#' PACKAGES: rd3tools, dotenv, os, json
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis import Molgenis
from dotenv import load_dotenv
from os import environ
import json

# set vars and init sessions for both Molgenis instances
load_dotenv()
rd3_acc = Molgenis(
    url=environ['MOLGENIS_HOST_ACC'],
    token=environ['MOLGENIS_TOKEN_ACC']
)
rd3_prod = Molgenis(
    url=environ['MOLGENIS_HOST_PROD'], 
    token=environ['MOLGENIS_TOKEN_PROD']
)

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Define Menu
menu = {
    "id":"main",
    "label":"Home",
    "items":[
        # KEEP
        {"type":"plugin","id":"home","label":"Home"},
        {
            "type":"plugin",
            "id":"redirect",
            "label":"Discovery Nexus",
            "params":"url=https://rdnexus.molgeniscloud.org/Discover/Index"
        },
        {"type":"plugin","id":"app-getstarted","label":"Get Started","params":""},
        
        #//////////////////////////////////////////////////////////////////////
        
        # EDIT THE ITEMS BELOW
        # Create dropdown menu for subjects
        {
            "type":"menu",
            "id":"subjects",
            "label":"Subjects",
            "items":[
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 1 Subjects",
                    "params":"entity=rd3_freeze1_subject&filter=patch==freeze1_original&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 1 Patch 1 Subjects",
                    "params":"entity=rd3_freeze1_subject&filter=patch==freeze1_patch1&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 1 Patch 3 Subjects",
                    "params":"entity=rd3_freeze1_subject&filter=patch==freeze1_patch3&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 2 Subjects",
                    "params":"entity=rd3_freeze2_subject&filter=patch==freeze2_original&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 2 Patch 1 Subjects",
                    "params":"entity=rd3_freeze2_subject&filter=patch==freeze2_patch1&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Novel Omics 1 Subjects",
                    "params":"entity=rd3_novelomics_subject&filter=patch==novelomics_original&hideselect=true&mod=data"
                }
            ]
        },
        # Create Dropdown for Samples
        {
            "type":"menu",
            "id":"samples",
            "label":"Samples",
            "items":[
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 1 Samples",
                    "params":"entity=rd3_freeze1_sample&filter=patch==freeze1_original&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 1 Patch 1 Samples",
                    "params":"entity=rd3_freeze1_sample&filter=patch==freeze1_patch1&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 2 Samples",
                    "params":"entity=rd3_freeze2_sample&filter=patch==freeze2_original&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Novel Omics 1 Samples",
                    "params":"entity=rd3_novelomics_sample&filter=patch==novelomics_original&hideselect=true&mod=data"
                }
            ]
        },
        
        # Create Dropdown for Labs
        {
            "type":"menu",
            "id":"labs",
            "label":"Labs",
            "items":[
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 1 Labs",
                    "params":"entity=rd3_freeze1_labinfo&filter=patch==freeze1_original&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 1 Patch 1 Labs",
                    "params":"entity=rd3_freeze1_labinfo&filter=patch==freeze1_patch1&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 2 Labs",
                    "params":"entity=rd3_freeze2_labinfo&filter=patch==freeze2_original&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Novel Omics 1 Labs WGS",
                    "params":"entity=rd3_novelomics_labinfo_wgs&filter=patch==novelomics_original&hideselect=true&mod=data"
                }
            ]
        },
        
        # Dropdown for Files
        {
            "type":"menu","id":"files","label":"Files",
            "items":[
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 1 Files",
                    "params":"entity=rd3_freeze1_file&filter=patch==freeze1_original&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 1 Patch 1 Files",
                    "params":"entity=rd3_freeze1_file&filter=patch==freeze1_patch1&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Freeze 2 Files",
                    "params":"entity=rd3_freeze2_file&filter=patch==freeze2_original&hideselect=true&mod=data"
                },
                {
                    "type":"plugin",
                    "id":"dataexplorer",
                    "label":"Novelomics 1 Files",
                    "params":"entity=rd3_novelomics_file&filter=patch==novelomics_original&hideselect=true&mod=data"
                }
            ]
        },
        #//////////////////////////////////////////////////////////////////////
        
        # KEEP
        {"type":"plugin","id":"navigator","label":"Navigate all tables","params":""},
        {
            "type":"menu","id":"admin","label":"Admin",
            "items":[
                {"type":"menu","id":"plugins","label":"Tools","items":[
                    {"type":"plugin","id":"scripts","label":"Scripts"},
                    {"type":"plugin","id":"metadata-manager","label":"Metadata Manager"},
                    {"type":"plugin","id":"importwizard","label":"Upload EMX, VCF"},
                    {"type":"plugin","id":"one-click-importer","label":"Upload using wizard"},
                    {"type":"plugin","id":"scheduledjobs","label":"Schedule Jobs"},
                    {"type":"plugin","id":"jobs","label":"View jobs"},
                    {"type":"plugin","id":"mappingservice","label":"Map and integrate data"},
                    {"type":"plugin","id":"sorta","label":"Code to ontologies"},
                    {"type":"plugin","id":"swagger","label":"API documentation"}
                    ]
                },
                {"type":"plugin","id":"settings","label":"Change setting","params":""},
                {"type":"plugin","id":"security-ui","label":"Group manager"},
                {"type":"plugin","id":"menumanager","label":"Change menu"},
                {"type":"plugin","id":"thememanager","label":"Change theme"},
                {"type":"plugin","id":"usermanager","label":"Manage users"},
                {"type":"plugin","id":"appmanager","label":"Add/remove apps"},
                {"type":"plugin","id":"feedback","label":"Feedback"},
                {"type":"menu","id":"advanced","label":"Advanced","items":[
                    {"type":"plugin","id":"questionnaires","label":"Questionnaires"},
                    {"type":"plugin","id":"permissionmanager","label":"Edit roles and permissions"},
                    {"type":"plugin","id":"indexmanager","label":"Manage indexing"},
                    {"type":"plugin","id":"logmanager","label":"Change log settings"}
                ]}
            ]
        },
        {"type":"plugin","id":"useraccount","label":"Account"}
    ]
}

# push to RD3
menuStringified = json.dumps(menu,separators=(',', ':'))
rd3_acc.update_one(entity='sys_set_app', id_='app', attr='molgenis_menu', value=menuStringified)
rd3_prod.update_one(entity='sys_set_app', id_='app', attr='molgenis_menu', value=menuStringified)
