#'////////////////////////////////////////////////////////////////////////////
#' FILE: data_common_analyses.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-11-17
#' MODIFIED: 2022-05-13
#' PURPOSE: get overviews on the database
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis import Molgenis
from dotenv import load_dotenv
import pandas as pd

# for local dev, set credentials
from dotenv import load_dotenv
from os import environ
load_dotenv()

# host=environ['MOLGENIS_PROD_HOST']
host=environ['MOLGENIS_ACC_HOST']
rd3=Molgenis(url=host)
rd3.login(
    username=environ['MOLGENIS_ACC_USR'],
    password=environ['MOLGENIS_ACC_PWD']
)

# What are the most commonly reported phenotypes
def getPhenotypicData(entityId):
    """Get Phenotypic Data
    Pull observed phenotype data from entities

    @param entityId RD3 table identifier
    @return list of dictionaries
    """
    return rd3.get(
        entity=entityId,
        attributes='phenotype',
        q='phenotype!=""',
        batch_size=10000
    )

def transformPhenotypicData(data):
    """Transform Phenotypic Data
    Reshape row-level phenotypic data
    @return list of dictionaries
    """
    data = []
    for row in data:
        if 'phenotype' in row:
            for value in row['phenotype']:
                data.append({
                    'id': value['id'],
                    'label': value['label']
                })
    return data

# pull data
phenotypicData = pd.DataFrame()
for entity in ['rd3_freeze1_subject','rd3_freeze2_subject']:
    phenotypicData.concat([phenotypicData, getPhenotypicData(entityId=entity)])

# identify the top five rows
observedPhenotypes = phenotypicData.value_counts(subset=['id']).reset_index().head()
observedPhenotypes.columns = ['id','count']
top5 = pd.concat([observedPhenotypes, phenotypicData], axis = 1, join = 'inner')

top5.columns = ['id','count','id2','label']
top5[['id','label']].to_csv('topfive_phenotypes.txt',index=False)
