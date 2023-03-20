#///////////////////////////////////////////////////////////////////////////////
# FILE: data_update_hpo.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-07
# MODIFIED: 2022-09-07
# PURPOSE: Download the latest HPO release and add new codes
# STATUS: in.progress 
# PACKAGES: **see below**
# COMMENTS: The purpose of this script is to download and extract the latest
# HPO release. These are available from the following gitHub repository
# https://github.com/obophenotype/human-phenotype-ontology. This script downloads
# the JSON format of a desired release, unpacks the data, and imports the missing
# codes into RD3.
#
# Previously, missing codes were manually verified and added to RD3 one-by-one.
# However, this process is no longer valid for releases that have many new codes
# as it isn't feasible to manually verify 100+ codes. It is better to run this
# script and revalidate codes. If there are any unknown codes, manually add those
# to RD3.
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.api.github import github
from dotenv import load_dotenv
from datatable import dt, f, fread
from tqdm import tqdm
import json
import re
import os

# connect to both rd3 instances
load_dotenv()
rd3_acc = Molgenis(os.environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(os.environ['MOLGENIS_ACC_USR'], os.environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(os.environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(os.environ['MOLGENIS_PROD_USR'], os.environ['MOLGENIS_PROD_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Find and download the latest HPO release from the github repo
# (we want the json format)

# Get the latest release
gh = github(owner='obophenotype', repo='human-phenotype-ontology')
gh.listReleases()

# find the latest release
tag = gh.releases[f.published_at == dt.max(f.published_at), 'tag_name'].to_list()[0][0]
gh.downloadRelease(outDir = 'downloads', tag_name = tag)

# find the latest json file and check to see if it exists
newReleaseHpoJson = f"downloads/{os.listdir('downloads')[0]}/hp.json"
os.path.exists(newReleaseHpoJson)

# open the file and extract the following datasets
#   1. nodes: specific HPO code entry
#   2. edges: the link between parent and child terms 
with open(newReleaseHpoJson, 'r') as file:
  graphs = json.load(file)['graphs']
  nodeset = graphs[0]['nodes']
  edges = graphs[0]['edges']
  file.close()

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process HPO Data
# In this step, we will extract the information needed for RD3. In addition, we
# need to identify the child-parent term relations and merge it with each HPO
# record.

# ~ 1a ~
# Process child-parent links
# edges is an array objects where each object links a parent term with a child
# term. For RD3, we want to link the parent code at the child level. Each item
# contains the following properties.
#   `obj`: the parent HPO code
#   `sub`: the child HPO code 
#
edgesDT = dt.Frame(edges)
# edgesDT[dt.re.match(f.sub, '.*HP_0010819'), :]

# get the HPO code
edgesDT['parentHpo'] = dt.Frame([
  os.path.basename(value)
  for value in edgesDT['obj'].to_list()[0]
])

edgesDT['childHpo'] = dt.Frame([
  os.path.basename(value)
  for value in edgesDT['sub'].to_list()[0]
])


# ~ 1b ~
# Extract HPO records
# process nodeset and extract relevant metadata
allHpoTerms = []
for node in tqdm(nodeset):
  if 'type' in node:
    if node['type'] == 'CLASS':
      extractedNodeData = {
        'id': os.path.basename(node['id']),
        'label': node.get('lbl'),
        'description': node.get('meta', {}).get('definition', {}).get('val', None),
        # 'codesystem': re.sub(r'([0-9\_])', '', os.path.basename(node['id'])),
        'iri': node.get('id'),
        'synonyms': ','.join(
          [synonym.get('val') for synonym in node.get('meta', {}).get('synonyms')]
        ) if node.get('meta', {}).get('synonyms') else None,
        'parents': None
      }

      # if there are parent-child links, add parent code(s)
      extractedNodeEdges = edgesDT[f.childHpo == extractedNodeData['id'], 'parentHpo']
      if extractedNodeEdges.nrows > 0:
        extractedNodeData['parents'] = ','.join(extractedNodeEdges.to_list()[0])
      
      allHpoTerms.append(extractedNodeData)
      
hpoDT = dt.Frame(allHpoTerms)

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Prepare for import into RD3
# To prevent a change in historical records, identify the cases that aren't in
# RD3.

# ~ 2a ~
# Get RD3 HPO reference table 
# The HPO table in ACC and PROD should be identical, but we will check it
# anyways. If the databases are out of sync, update them before proceding.
hpo_acc = dt.Frame(rd3_acc.get('rd3_phenotype', attributes='id,label', batch_size=10000))
hpo_prod = dt.Frame(rd3_prod.get('rd3_phenotype', attributes='id,label', batch_size=10000))

if hpo_acc.nrows != hpo_prod.nrows:
  raise Warning('Phenotype reference tables are out of sync. Fix this before proceding')

# if everything looks good, then work with the prod table only
hpoCodes = hpo_prod['id'].to_list()[0]
del hpo_acc

# ~ 2b ~
# Identify new codes

# create a bool attrib that indicates a code is not in RD3
hpoDT['codeIsNew'] = dt.Frame([
  value not in hpoCodes
  for value in tqdm(hpoDT['id'].to_list()[0])
])

# summarise data
hpoDT[:, dt.count(), dt.by('codeIsNew')]

# create a subset with only new codes
newHpoDT = hpoDT[f.codeIsNew, :]

hpo_prod.nrows
hpo_prod.nrows + newHpoDT.nrows

# import new codes into RD3
rd3_acc.importDatatableAsCsv(pkg_entity = 'rd3_phenotype', data = newHpoDT)
rd3_prod.importDatatableAsCsv(pkg_entity = 'rd3_phenotype', data = newHpoDT)

#///////////////////////////////////////////////////////////////////////////////

# ~ 999 ~
# run check against missing cases

missingCodesDT = fread('data/freeze3_hpo_codes.csv')
missingCodesDT['status'] = dt.Frame([
  value in newHpoDT['id'].to_list()[0]
  for value in missingCodesDT['code'].to_list()[0]
])

missingCodesDT[:, dt.count(), dt.by(f.status)]