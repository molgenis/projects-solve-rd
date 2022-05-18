#'////////////////////////////////////////////////////////////////////////////
#' FILE: molgenis_app_set_menu.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-01-18
#' MODIFIED: 2022-05-13
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
rd3_acc = Molgenis(url=environ['MOLGENIS_ACC_HOST'])
rd3_prod = Molgenis(url=environ['MOLGENIS_PROD_HOST'])

rd3_prod.login(
    username=environ['MOLGENIS_PROD_USR'],
    password=environ['MOLGENIS_PROD_PWD']
)

rd3_acc.login(
    username=environ['MOLGENIS_ACC_USR'],
    password=environ['MOLGENIS_ACC_PWD']
)

# read contents of the menu config file
with open('rd3/molgenis_menu.json', 'r') as file:
    menu=file.read()
    file.close()

# parse json and stringify
molgenisMenuJson=json.loads(menu)
menuStringified = json.dumps(molgenisMenuJson)

# push to RD3
rd3_acc.update_one(entity='sys_set_app', id_='app', attr='molgenis_menu', value=menuStringified)
rd3_prod.update_one(entity='sys_set_app', id_='app', attr='molgenis_menu', value=menuStringified)
