#///////////////////////////////////////////////////////////////////////////////
# FILE: data_list_files_by_release.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-01
# MODIFIED: 2022-09-01
# PURPOSE: list files on the cluster at a given path and import into rd3_portal
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import statusMsg
from rd3.utils.clustertools import clustertools
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
from tqdm import tqdm
load_dotenv()

# set paths
currentRelease = 'freeze1'
clusterReleasePath = f"{environ['CLUSTER_BASE']}/{currentRelease}"
paths = {
  'ped': clusterReleasePath + '/ped',
  'phenopacket': clusterReleasePath + '/phenopacket',
  'gvcf': clusterReleasePath + '/gvcf',
  'bam': clusterReleasePath + '/bam',
}


# connect to RD3
statusMsg('Connecting to RD3 instances...')
rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Generate list of all files in the current release

statusMsg('Getting PED files at',paths['ped'],'....')

# ~ 1a ~
# Get PED data
ped = dt.Frame(
  clustertools.listFiles(
    path=paths['ped'],
    filter=r'(.ped)$'
  )
)

ped['filetype'] = 'ped'
ped['familyID'] = dt.Frame([
  filename.split('.')[0]
  for filename in ped['filename'].to_list()[0]
])

#///////////////////////////////////////

# ~ 1b ~
# Get Phenopacket data

statusMsg('Getting Phenopacket files at', paths['phenopacket'],'...')
phenopacket = dt.Frame(
  clustertools.listFiles(
    path=paths['phenopacket'],
    filter=r'(.json)$'
  )
)

phenopacket['filetype'] = 'phenopacket'
phenopacket['subjectID'] = dt.Frame([
  filename.split('.')[0]
  for filename in phenopacket['filename'].to_list()[0]
])

# ~ 1c ~
# Get BAM data
statusMsg('Looking for bam files at', paths['bam'])
bambase = clustertools.listFiles(path=paths['bam'])
bam = dt.Frame()

# ~ 1d ~
# Get GVCF data
# NOTE: this still will take a while to run
statusMsg('Looking for gVCF files at', paths['gvcf'])
gvcfbase =clustertools.listFiles(path=paths['gvcf'])

gvcf = dt.Frame()
if gvcfbase:
  statusMsg('Getting gvcf files')
  for row in tqdm(gvcfbase):
    gvcf = dt.rbind(
      gvcf,
      dt.Frame(
        clustertools.listFiles(
          path=row['filepath'],
          filter=r'(.vcf|.vcf.gz|.cip)$',
          quietly=True
        )
      )
    )
    
  gvcf['filetype'] = 'gvcf'
  gvcf['experimentID'] = dt.Frame([
    filename.split('.')[0]
    for filename in gvcf['filename'].to_list()[0]
  ])

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Import into RD3

statusMsg('Importing data into both RD3 instances....')

# combine into one dataset
files = dt.rbind(ped,phenopacket,gvcf,bam,force=True)
files['release'] = currentRelease

# remove duplicate rows (I'm not sure why there would be any, but there are)
statusMsg('Checking for duplication values (rows:',files.nrows,')')
if dt.unique(files['filepath']).nrows != files.nrows:
  statusMsg(
    'Duplicate rows detected: unique paths',
    dt.unique(files['filepath']).nrows,
    'vs', files.nrows
  )
  files = files[:, dt.first(f[:]), dt.by(f.filepath)]

statusMsg('Final row count:',files.nrows)

# save file incase of import errors
localCopyPath=f"data/rd3_portal_cluster_{currentRelease}.csv"
statusMsg('Saving backup to',localCopyPath,'....')
files.to_csv(localCopyPath)

statusMsg('Importing data....')
rd3_acc.importDatatableAsCsv(
  pkg_entity=f'rd3_portal_cluster_{currentRelease}',
  data = files
)

rd3_prod.importDatatableAsCsv(
  pkg_entity=f'rd3_portal_cluster_{currentRelease}',
  data = files
)

rd3_prod.logout()
rd3_acc.logout()