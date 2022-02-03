#'////////////////////////////////////////////////////////////////////////////
#' FILE: data_common_analyses.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-11-17
#' MODIFIED: 2021-11-17
#' PURPOSE: get overviews on the database
#' STATUS: in.progress
#' PACKAGES: molgenis.client
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import molgenis.client as molgenis
import pandas as pd

host = ''
usr = ''
pwd = ''

rd3 = molgenis.Session(host)
rd3.login(usr, pwd)


#//////////////////////////////////////

# What are the most commonly reported phenotypes

def getPhenotypicData(entities: list = None):
    """Get Phenotypic Data
    
    Pull observed phenotype data from entities
    
    Attributes:
    
        entities (list) : a list containing entity IDs
    
    """
    data = []
    for entity in entities:
        print('Pulling data from: {}'.format(entity))
        raw = rd3.get(
            entity = entity,
            attributes = 'phenotype',
            q = 'phenotype!=""',
            batch_size = 10000
        )
        for r in raw:
            if 'phenotype' in r:
                for p in r['phenotype']:
                    data.append({
                        'id': p['id'],
                        'label': p['label']
                    })
    print('Found {} records'.format(len(data)))
    return data

# pull data
phenotype = pd.DataFrame(
    getPhenotypicData(entities=['rd3_freeze1_subject','rd3_freeze2_subject'])
)

observedPhenotypes = phenotype.value_counts(subset=['id']).reset_index().head()
observedPhenotypes.columns = ['id','count']

top5 = pd.concat(
    [observedPhenotypes, phenotype],
    axis = 1,
    join = 'inner'
)

top5.columns = ['id','count','id2','label']
top5[['id','label']].to_csv('~/Desktop/rd3_data_for_demo.txt',index=False)