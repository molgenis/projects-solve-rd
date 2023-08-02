#///////////////////////////////////////////////////////////////////////////////
# FILE: coverage_report.py
# AUTHOR: David Ruvolo
# CREATED: 2023-06-29
# MODIFIED: 2023-06-29
# PURPOSE: summary script for coverage report
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import flattenDataset
from rd3.api.molgenis2 import Molgenis
from datatable import dt, f

from dotenv import load_dotenv
from os import environ

load_dotenv()
rd3=Molgenis(url=environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'],environ['MOLGENIS_PROD_PWD'])

#///////////////////////////////////////

# ~ 0 ~
# Pull All Subject Metadata


columns=[
  'subjectID', 'sex1', 'fid', 'mid', 'pid', 'clinical_status', 'solved',
  'disease', 'phenotype', 'hasNotPhenotype', 'organisation', 'ERN', 'partOfRelease'
]

rawsubjects = rd3.get('solverd_subjects',attributes=','.join(columns),batch_size=10000)
subjectsflattened = flattenDataset(rawsubjects, columnPatterns='subjectID|id|value')
subjectsDT=dt.Frame(subjectsflattened)


# summarise dataset
total = subjectsDT['subjectID'].nrows

stats = []
for index, column in enumerate(columns):
  observed = subjectsDT[f[column] != None, f[column]].nrows
  stats.append({
    'id': f"coverage-subjects-{index}",
    'table': 'Subjects',
    'attribute': column,
    'expected': total,
    'observed': observed,
    'percent': round(observed / total, 4)
  })

# import
statsDT = dt.Frame(stats)

rd3.importDatatableAsCsv('rd3stats_coverage', statsDT)