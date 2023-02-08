#///////////////////////////////////////////////////////////////////////////////
# FILE: index.py
# AUTHOR: David Ruvolo
# CREATED: 2023-02-08
# MODIFIED: 2023-02-08
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
#///////////////////////////////////////////////////////////////////////////////

import molgenis.client as molgenis

# connect to RD3
rd3 = molgenis.Session('https://solve-rd.gcc.rug.nl/api/')
rd3.login(username='...', password='...')

#///////////////////////////////////////

# ~ 1 ~
# Example 1: How do I find samples submitted by a specific data provider?

samplesByProvider = rd3.get(
  entity='solverd_samples',
  q='ERN=in=(ern_genturis,ern_rita)'
)

#///////////////////////////////////////

# ~ 2 ~
# Example 2: How do I find special combinations of omics samples?
# Conduct an "and" search with "or"

epigenomeSamples = rd3.get(
  entity='solverd_samples',
  q='(partOfRelease==novelomics_epigenome;partOfRelease=in=(novelsrwgs_original,novellrwgs_original))'
)

#///////////////////////////////////////

# ~ 3 ~
# Example 3: How do I....?


#///////////////////////////////////////

# ~ 4 ~
# Example 4: How do I find subjects with RNAseq and WES/WGS data?

# pull WGS/WES release IDs. These start with "freeze"
releases = rd3.get(entity='solverd_info_datareleases')
releasesForQuery = ','.join([ row['id'] for row in releases if 'freeze' in row['id'] ])

# create query
query = f"(partOfRelease==novelrnaseq_original;partOfRelease=in=({releasesForQuery})"

# run search
rnaSeqSubjects = rd3.get(entity='solverd_subjects', q=query)
