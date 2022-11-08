
from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt, f
from os import environ

load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

# get latest cluster metadata
phenopackets = dt.Frame(
  rd3.get(
    'rd3_portal_cluster_phenopacket',
    attributes='phenopacketsID,subjectID'
  )
)

ped = dt.Frame(
  rd3.get(
    'rd3_portal_cluster_ped',
    attributes = 'id,pedID,subjectID'
  )
)

# reproduce query
patches = dt.Frame(rd3.get('rd3_patch', attributes='id', q='id=like=novel'))['id']
q = f"patch=in=({ ','.join(patches['id'].to_list()[0]) })"

data = dt.Frame(rd3.get('rd3_overview', q = q,attributes='subjectID,fid'))

missingFIDs = data[f.fid==None, :]

missingFIDs['hasPhenopacket'] = dt.Frame([
  id in phenopackets['subjectID'].to_list()[0]
  for id in missingFIDs['subjectID'].to_list()[0]
])

missingFIDs['hasPED'] = dt.Frame([
  id in ped['subjectID'].to_list()[0]
  for id in missingFIDs['subjectID'].to_list()[0]
])

missingFIDs[:, dt.count(), dt.by(f.hasPhenopacket)]
missingFIDs[:, dt.count(), dt.by(f.hasPED)]