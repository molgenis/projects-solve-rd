#///////////////////////////////////////////////////////////////////////////////
# FILE: examples.py
# AUTHOR: David Ruvolo
# CREATED: 2023-02-08
# MODIFIED: 2023-02-13
# PURPOSE: Code examples presented in the workshop presentation slides
# STATUS: stable
# PACKAGES: molgenis.client
# COMMENTS: Install the molgenis client from pypi. See the github repository
# for installation information. https://github.com/molgenis/molgenis-py-client/.
# The client can be installed using the following command.
#
#     pip install molgenis-py-client
#
# The filters use RSQL. For information on how to structure queries, see this
# page: https://molgenis.gitbook.io/molgenis/interoperability/guide-rsql.
#
# If you are requesting large amounts of data, increase the `batch_size`.
#
#     rd3.get(entity='...', batch_size=1000)
#
# This script also uses a .env file to store and use credentials. Create
# a new file and enter your information like so:
#
#  MOLGENIS_PROD_USR="...."
#  MOLGENIS_PROD_PWD="...."
#
#///////////////////////////////////////////////////////////////////////////////

import molgenis.client as molgenis
from dotenv import load_dotenv
from os import environ
load_dotenv()

# connect to RD3
rd3 = molgenis.Session('https://solve-rd.gcc.rug.nl/api/')
rd3.login(
  username=environ['MOLGENIS_PROD_USR'],
  password=environ['MOLGENIS_PROD_PWD']
)

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Example 1: How do I find SR-RNAseq samples submitted by ERN-RND?
# This information is found in the samples table. We will need a few pieces of
# information to retrieve the metadata.
# 
# 1. The entity name for the samples table
# 2. Name of the column where ERN affiliation is stored
# 3. The RD3 identifier for ERN-RND
#

data = rd3.get(
  entity='solverd_samples',
  q='ERN==ern_rnd;partOfRelease==novelsrrnaseq_original',
  batch_size=1000
)

len(data)

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Example 2: How do I find special combinations of omics samples?
# In some situtations, you would like to find combinations of samples. For example,
# which Epigenome samples were also included in LR-WGS or SR-WGS? It is possible to 
# run searches like these, but we will need some information first.
# 
# 1. The entity name for the samples table
# 2. Name of the column where sample type (i.e., omics) releases are stored
# 3. The RD3 identifiers for Epigenome, LR-WGS, and SR-WGS releases.
#

# create a fiter for Epigenome records
queryPart1='partOfRelease==novelepigenome_original'

# create a filter that looks for SR-WGS or LR-WGS records
queryPart2='partOfRelease=in=(novelsrwgs_original,novellrwgs_original)'

# create filter
query = f'({queryPart1};{queryPart2})'

data = rd3.get(
  entity='solverd_samples',
  q=query,
  batch_size=1000
)

len(data)

#///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Example 3: How do I find subjects with RNAseq and WES/WGS data?
# In this example, we would like to find subjects have SR-RNAseq metadata and
# WES or WGS metadata. This information is stored in the subjects table. WES
# and WGS metadata is part of the main Solve-RD data freezes. Since there are
# a number of data freezes and updates, we will retrieve this information
# via the API and extract the IDs programmatically.

# ~ 3a ~
# Find release identifiers
# Releases are stored in the Data Releases table `solverd_info_datareleases`.
# All WES and WGS releases and updates are prefixed with "freeze". We can 
# extract the IDs by search for releases that are part of a freeze. We will
# also extract the SR-RNAseq release ID.
releases = rd3.get(entity='solverd_info_datareleases')

# extract release IDs that match `Freeze` and `SR-RNAseq`
releaseIDs=[]
for row in releases:
  if 'Freeze' in row['name']:
    releaseIDs.append(row['id'])
  if 'SR-RNAseq' in row['name']:
    srRnaSeqId = row['id']

# ~ 3b ~
# Create query and retrieve records
query=f"(partOfRelease=={srRnaSeqId};partOfRelease=in=({','.join(releaseIDs)}))"

subjects = rd3.get(entity='solverd_subjects', q=query, batch_size=1000)

#///////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# Example 4 & 5: How do I find combined SR-WGS and SR-RNAseq samples? Are there
# any files?
# In this example, we are looking for SR-WGS and SR-RNAseq samples. Since these are 
# two samples, we need to conduct the search in the subjects table. This will show 
# give us the subject IDs of those with SR-WGS and SR-RNAseq metadata, which we can 
# use to retrieve the sample metadata.
# 
# 1. Name of the subjects table
# 2. Name of the column where sample type (i.e., omics) releases are stored
# 3. The RD3 identifiers for the SR-WGS and SR-RNAseq releases
# 
# The entity name for the subjects table is `solverd_subjects`, and the release IDs 
# are `novelsrwgs_original` and `novelsrrnaseq_original`.
# 
# Sample metadata can be retrieved in two steps: 1) retrieve subject IDs that have 
# SR-WGS and SR-RNAseq metadata and 2) retrieve samples using subject IDs found in 
# step one.

# ~ 4a ~
# Find Subjects
# Query the subjects table. Since we only need the subject identifiers, we will
# use the `attributes` parameter to select the subject ID column.
subjects = rd3.get(
  entity='solverd_subjects',
  q='(partOfRelease==novelsrwgs_original;partOfRelease==novelsrrnaseq_original)',
  attributes='subjectID',
  batch_size=1000
)

# extract subject IDs and format as a comma-separated string for the API request
subjectIDs = ','.join([row['subjectID'] for row in subjects])


# ~ 4b ~
# Retrieve sample metadata
# Using the subjectIDs extracted in step 1 and the release IDs, create a new
# filter. In the samples table, the reference to subject can be found in the
# column `belongsToSubject`

subjectQuery=f"belongsToSubject=in=({subjectIDs})"
releaseQuery="partOfRelease=in=(novelsrrnaseq_original,novelsrwgs_original)"
query = f"({subjectQuery};{releaseQuery})"

samples = rd3.get(entity='solverd_samples', q=query, batch_size=1000)

len(samples)

# ~ 4c ~
# Retrieve files
# Using the subjectIDs extracted in step 1 and the release IDs, create a new
# filter. In the files table, the reference to subject can be found in the
# column `subjectID`

subjectQuery=f"subjectID=in=({subjectIDs})"
releaseQuery="partOfRelease=in=(novelsrrnaseq_original,novelsrwgs_original)"
query = f"({subjectQuery};{releaseQuery})"

files = rd3.get(entity='solverd_files', q=query, batch_size=10000)

len(files)


rd3.logout()
