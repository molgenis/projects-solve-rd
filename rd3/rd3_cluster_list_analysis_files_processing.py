#///////////////////////////////////////////////////////////////////////////////
# FILE: rd3_cluster_list_analysis_files_processing.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-15
# MODIFIED: 2022-09-15
# PURPOSE: Read compiled csv and prep for import into Molgenis
# STATUS: in.progress
# PACKAGES: NA
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.utils.clustertools import clustertools
from datatable import dt, f
from dotenv import load_dotenv
from os import environ
import re

load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

path='/groups/solve-rd/tmp10/dcruvolo/rd3_snv-indel_contents.csv'
data = dt.Frame(clustertools.readCsv(path))


def extractErn(value):
  """Match Pattern Dictionary
  Test a dictionary of patterns and return a value
  
  @param value string to search
  @param patterns a dictionary where each key is as pattern
  """
  search = re.search(
    pattern = r'(([/_-]?(nmd|genturis|epicare|ithaca|rita|rnd)[/_]?)|ern-rita)',
    string = value.lower()
  )
  if search:
    return re.sub(r'([/_-])','', search.group()).upper()
  else:
    return None

def recodeFileExtension(value):
  if re.search(r'(.tsv(.)?)', value):
    return 'tsv'  
  if re.search(r'(.hcdiff(.)?)', value):
    return 'hcdiff'
  if re.search(r'(.vcf|.vcf.gz)', value):
    return 'vcf'


#///////////////////////////////////////

# ~ 0 ~
# Validate data and transform variables

# filter data:
#  - remove all sftp files
#  - remove all files where the size is 0
fileData = data[dt.re.match(f.path, '.*/sftp/.*')==False, :][f.size != '0', :]


# ~ 0a ~
# Make sure inode numbers are unique
# If they aren't, pull the data into pandas and get the cumulative count
# per inode (i.e., file), and then bind inode and sequence number if the
# index is not 0.

dt.unique(fileData['inode']).nrows == fileData.nrows
fileData[:, dt.count(), dt.by(f.inode)][f.count > 1, :]

# get cumulative count by inode
fileDataPD = fileData.to_pandas()
fileDataPD['cumulativeCount'] = fileDataPD.groupby(['inode']).cumcount()
fileData = dt.Frame(fileDataPD)

# reformat inode with cumulative count
fileData['inode'] = dt.Frame([
  f'{tuple[0]}.{tuple[1]}' if tuple[1] != 0 else tuple[0]
  for tuple in fileData[:, (f.inode, f.cumulativeCount)].to_tuples()
])

dt.unique(fileData['inode']).nrows == fileData.nrows


# ~ 0b ~
# Extract ERN from file path
fileData['ern'] = dt.Frame([
  extractErn(d)
  for d in fileData['path'].to_list()[0]
])

# fileData[:, (f.ern,f.path)]
# dt.unique(fileData['ern'])
# fileData[:, dt.count(), dt.by(f.ern)]
# fileData[:, dt.count(), dt.by(f.ern)][:, dt.sum(f.count)]

# ~ 0c ~
# Check file extensions
dt.unique(fileData[:, f.extension])

acceptedFiles = [
  'csv',
  'hcdiff','list','log','metainfo','txt','tsv','xlsx'
]

fileData['extension'] = dt.Frame([
  recodeFileExtension(tuple[1])
  if tuple[0] not in acceptedFiles else tuple[0]
  for tuple in fileData[:, ['extension', 'name']].to_tuples()
])

# dt.unique(fileData[:, f.extension])

#///////////////////////////////////////

# ~ 1 ~
# Import

# rd3.delete('rd3_cluster_results_snvindel')
rd3.importDatatableAsCsv(
  pkg_entity='rd3_cluster_results_snvindel',
  data = fileData
)