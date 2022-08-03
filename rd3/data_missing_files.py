#'////////////////////////////////////////////////////////////////////////////
#' FILE: data_missing_files.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-08-03
#' MODIFIED: 2022-08-03
#' PURPOSE: find subjects with missing files
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from os import environ
from datatable import dt, f
import pandas as pd

load_dotenv()
rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


# get novel omics patches only
patches = dt.Frame(rd3.get('rd3_patch'))
novelOmicsReleases = patches[dt.re.match(f.id, 'novel.*'), 'id'].to_list()[0]

# set target columns and query
targetColumns=['clinical_status', 'sex1', 'phenotype', 'hasNotPhenotype', 'disease']
query=f"patch=in=({','.join(novelOmicsReleases)})"

data = rd3.get(
  'rd3_overview',
  q=query,
  attributes=f"subjectID,patch,{','.join(targetColumns)}",
  batch_size = 10000
)

# determine
status = []
for row in data:
  subjectstatus = {'subjectID': row['subjectID'], 'likelyMissingPedFile':None, 'likelyMissingPhenopacketFile': None}
  for column in targetColumns:
    subjectstatus[column] = False
    if row.get(column) is not None:
      if bool(row.get(column)):
        subjectstatus[column] = True
  subjectstatus['patch'] = ','.join([patch['id'] for patch in row['patch']])
  subjectstatus['url']=f"/menu/main/dataexplorer?entity=rd3_overview&filter=subjectID=={row['subjectID']}&hideselect=true&mod=data"
  status.append(subjectstatus)
  
statusDT = dt.Frame(status)

statusDT['likelyMissingPedFile'] = dt.Frame([
  not bool(d[0]) and not bool(d[1])
  for d in statusDT[:, (f.clinical_status, f.sex1)].to_tuples()
])

statusDT['likelyMissingPhenopacketFile'] = dt.Frame([
  not bool(d[0]) and not bool(d[1]) and not bool(d[2])
  for d in statusDT[:, (f.phenotype, f.hasNotPhenotype, f.disease)].to_tuples()
])

statusDT['url'] = dt.Frame([
  f"=HYPERLINK(\"https://solve-rd.gcc.rug.nl{d[1]}\", \"View {d[0]} in RD3\")"
  for d in statusDT[:, (f.subjectID, f.url)].to_tuples()
])


# define the codebook
codebook = dt.Frame(
  column=[
    'subjectID',
    'patch',
    'url',
    'clinical_status',
    'sex1',
    'phenotype',
    'hasNotPhenotype',
    'disease',
    'likelyMissingPedFile',
    'likelyMissingPhenopacketFile'
  ],
  description = [
    'a unique RD3 participant identifier', # subjectID
    'the RD3 releases associated with a subject', # patch
    'link to view the record in RD3', # url
    'an indication if the participant is affected (from PED)', # clinical_status
    'indication of the subject\'s sex (from PED)', # sex1
    'observed phenotypes reported (from Phenopackets)', # phenotype
    'unobserved phenetypes reported (from Phenopackets)', # hasNotPhenotype
    'reported disease codes (ORDO, MIM, etc.; from Phenopackets)', # disease
    'PED file is likely missing given that clinical_status and sex1 are blank',
    'Phenopacket is likely missing given that phenotype columns and disease are blank'
    
  ]
)

# write sheets
file = 'data/rd3_novelomics_missing_files.xlsx'
with pd.ExcelWriter(file) as writer:
  statusDT.to_pandas().to_excel(writer, sheet_name='data', index=False)
  codebook.to_pandas().to_excel(writer, sheet_name='codebook', index=False)
