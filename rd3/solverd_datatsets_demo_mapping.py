"""Create datasets"""

import random
from os import environ
from dotenv import load_dotenv
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, flatten_data
from datatable import dt, f
load_dotenv()

print2('Connecting to RD3....')
rd3 = Molgenis(url=environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


# pull data
subjects_raw = rd3.get(
    'solverd_subjects',
    attributes='subjectID,sex1,disease,phenotype',
    batch_size=10000
)

# flatten data
subjects_flat = flatten_data(subjects_raw, 'id')
subjects_dt = dt.Frame(subjects_flat)

ordo = []
for entry in subjects_dt[f.disease != None, 'disease'].to_list()[0]:
    for code in entry.split(','):
        if code not in ordo:
            ordo.append(code)


hpo = []
for entry in subjects_dt[f.phenotype != None, 'phenotype'].to_list()[0]:
    for code in entry.split(','):
        if code not in hpo:
            hpo.append(code)


datasets = []
for index in range(0, 10):
    total = random.randint(1000, 10000)
    rate = random.random()
    hpoLength = random.randint(0, 10)
    ordoLength = random.randint(0, 10)
    datasets.append({
        'id': f'test-dataset-{index}',
        'datasetType': 'processed',
        'numberOfRows': total,
        'numberOfMales': round(total * rate),
        'numberOfFemales': (total - round(total * rate)),
        'hpoCodes': ','.join(random.sample(hpo, k=hpoLength)),
        'ordoCodes': ','.join(random.sample(ordo, k=ordoLength))
    })

datasets_dt = dt.Frame(datasets)
rd3.import_dt('solverd_info_datasets', datasets_dt)
