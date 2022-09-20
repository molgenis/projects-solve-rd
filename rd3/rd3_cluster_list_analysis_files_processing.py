#///////////////////////////////////////////////////////////////////////////////
# FILE: rd3_cluster_list_analysis_files_processing.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-15
# MODIFIED: 2022-09-19
# PURPOSE: Read compiled csv and prep for import into Molgenis
# STATUS: stable
# PACKAGES: Molgenis2, clustertools, datatable, dotenv, os, re
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.utils.clustertools import clustertools
from datatable import dt, f, fread, as_type
from dotenv import load_dotenv
from os import environ
import re

load_dotenv()
rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

def extractPattern(value):
  """Match Pattern Dictionary
  Test a dictionary of patterns and return a value
  
  @param patterns a search pattern
  @param value string to search
  """
  mappings = {
    'ERNRITA': 'ern_rita',
    'GENTURIS': 'ern_genturis',
    'ITHACA': 'ern_ithaca',
    'NMD': 'ern_euro_nmd',
    'RND': 'ern_rnd',
    'RITA': 'ern_rita'
  }
  search = re.search(
    r'(([/_-]?(nmd|genturis|epicare|ithaca|rita|rnd)[/_]?)|ern-rita)',
    string = value.lower()
  )
  if search:
    match = re.sub(r'([/_-])','', search.group()).upper() 
    try:
      return mappings[match]
    except KeyError as error:
      print(error)
      return match
  else:
    return None

def recodeFileExtension(value):
  if re.search(r'(.tsv(.)?)', value):
    return 'tsv'  
  if re.search(r'(.hcdiff(.)?)', value):
    return 'hcdiff'
  if re.search(r'(.vcf|.vcf.gz)', value):
    return 'vcf'
  if re.search(r'(.txt.tmp)', value):
    return 'txt'

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Get data and transform variables

# get known file types
knownFileTypes = dt.Frame(rd3.get('rd3_cluster_filetypes'))['value'].to_list()[0]

# read data
cluster = clustertools('airlock+gearshift')
path = '/home/umcg-druvolo/data/rd3_meta-analysis_contents.csv'
data = dt.Frame(cluster.readCsv(path))

# filter data:
#  - remove all sftp files
#  - remove all files where the size is 0
fileData = data[dt.re.match(f.path, '.*/sftp/.*')==False, :]
fileData = fileData[f.size != '0', :]
# fileData = fileData[f.size != 0, :]


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

# check unique IDs against total rows once more
dt.unique(fileData['inode']).nrows == fileData.nrows

#///////////////////////////////////////

# ~ 0b ~
# Extract ERN from file path
fileData['ern'] = dt.Frame([
  extractPattern(d)
  for d in fileData['path'].to_list()[0]
])

# check mappings: all ern values should be 'ern_*'
# fileData[:, (f.ern,f.path)]
# fileData[f.ern==None, (f.ern,f.path)]
# dt.unique(fileData['ern'])
# fileData[:, dt.count(), dt.by(f.ern)]
# fileData[:, dt.count(), dt.by(f.ern)][:, dt.sum(f.count)]


# ~ 0c ~
# Check file extensions

fileData['extension'] = dt.Frame([
  type.lower() if type is not None else type
  for type in fileData['extension'].to_list()[0]
])

# check to see which files are known. Investigate/add new types
uniqueFileTypes = dt.unique(fileData[:, f.extension])
uniqueFileTypes['isKnown'] = dt.Frame([
  type in knownFileTypes
  for type in uniqueFileTypes['extension'].to_list()[0]
])

uniqueFileTypes[f.isKnown == False, :]

# for new file add them here and import
# fileData[f.extension=='tbi','name']
# filetypes = dt.Frame(value=['err', 'hdf5', 'log'])
# rd3.importDatatableAsCsv('rd3_cluster_filetypes', filetypes)
# knownFileTypes.extend(filetypes['value'].to_list()[0])

# recode known variations: after recoding run the uniqueFileTypes section
# to make sure all extensions were handled correctly. If not, repeat until
# all distinct values are accounted for.
fileData['extension'] = dt.Frame([
  recodeFileExtension(tuple[1])
  if tuple[0] not in knownFileTypes else tuple[0]
  for tuple in fileData[:, ['extension', 'name']].to_tuples()
])

# dt.unique(fileData[:, f.extension])

#///////////////////////////////////////

# ~ 1 ~
# Import

pkgEntity = 'rd3_cluster_results_meta'
# rd3.delete(pkgEntity)

del fileData['cumulativeCount']

# import directly if files is small enough (~500k or so)
rd3.importDatatableAsCsv(pkgEntity, fileData)

# or import in batches if file exceeds maximum import size
# incrementBy = 350000
incrementBy = 500000
for batch in range(0, fileData.nrows, incrementBy):
  maxrows = batch + incrementBy
  print('Importing rows',batch,'to',maxrows)
  if maxrows > fileData.nrows:
    maxrows = fileData.nrows
  batchData = fileData[range(batch,maxrows), :]
  # rd3.importDatatableAsCsv(pkgEntity, batchData)
  # batchData.to_csv(
  #   f"data/batches/rd3_cluster_results_str_batch_{batch/incrementBy}",
  #   quoting=csv.QUOTE_ALL
  # )
  
import numpy as np
import csv
fileData[f.inode=='144115206077850660.3', :]
fileData[f.name=='E022139_TBP.pdf', :].to_pandas().replace({np.nan:None}).to_dict('records')

#///////////////////////////////////////////////////////////////////////////////

# ~ 999 ~
# Alternative Imports for large files

# alternatively save to csv
# fileData.to_csv('data/rd3_cluster_results_cnv.csv')
# fileData = fread('data/rd3_cluster_results_str.csv')

# remove scientific notation
# fileData[:, as_type(f.inode, dt.Type.str32)]
# fileData['inode'] = dt.Frame([ f"{id:.0f}" for id in fileData['inode'].to_list()[0] ])

# reapply inode counts
# fileDataPD = fileData.to_pandas()
# fileDataPD['cumulativeCount'] = fileDataPD.groupby(['inode']).cumcount()
# fileData = dt.Frame(fileDataPD)
# fileData['inode'] = dt.Frame([
  # f'{tuple[0]}.{tuple[1]}' if tuple[1] != 0 else tuple[0]
  # for tuple in fileData[:, (f.inode, f.cumulativeCount)].to_tuples()
# ])

# check unique IDs against total rows once more
# dt.unique(fileData['inode']).nrows == fileData.nrows

# import data in loop
# incrementBy = 250000
# for batch in range(0, fileData.nrows, incrementBy):
#   maxrows = batch+incrementBy
#   print('Importing rows', batch, 'to', maxrows)
#   if (batch+incrementBy) > fileData.nrows:
#     maxrows = fileData.nrows
#   batchData = fileData[range(batch,maxrows), :]
#   rd3.importDatatableAsCsv('rd3_cluster_results_cnv', batchData)


# testPD = fileData.to_pandas()
# testPD.index[testPD['inode']=='144115206094591936.212'].tolist()
# rd3.importDatatableAsCsv(
#   'rd3_cluster_results_cnv', 
#   fileData[2667010,:]
# )