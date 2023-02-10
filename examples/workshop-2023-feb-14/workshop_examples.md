# Solve-RD Workshop on RD3

Date: 14 February 2023 @ 14:00

## Get Started

The following examples are python reproductions of the examples presented in the RD3 workshop. These examples require information on table identifiers, internal column names, and specific lookup values. Each example will provide this information along with the code. There are more tables and columns available in the database. For more information, have a look at the [RD3 Schema](https://github.com/molgenis/molgenis-solve-rd/blob/main/model/schemas/rd3.md).

> **Note**: You will need an API account to work with RD3. Please follow the procedures outlined in the workshop slides to request an account.

### Installing the molgenis client

These examples use the [MOLGENIS Python client](https://github.com/molgenis/molgenis-py-client/). Install the molgenis client using pip.

```bash
pip install molgenis-py-client
```

### Connecting to RD3

Import the MOLGENIS module, and use your API account credentials to sign in. If you do not have an RD3 API account, please follow the steps outlined in the workshop slides.

```py
import molgenis.client as molgenis

rd3 = molgenis.Session('https://solve-rd.gcc.rug.nl/api/')
rd3.login(username='...', password='...')
```

### Querying tables

To pull data from a specific table, you will need the names of the database and the table. For RD3, the database name will always be `solverd`. Additional tables and the internal names can be found in [entities schema](https://github.com/molgenis/molgenis-solve-rd/blob/main/model/schemas/rd3.md#entities).

When retrieving data from a table, use the `entity` parameter and format the value using the `<database>_<name>` format. If you were retrieve metadata from the subjects table, you would write the request like so:

```py
results = rd3.get(entity='solverd_subjects')
```

### Tips for using the API

Use the option `batch_size` to increase the number records retrieved at once. By default, this number is 100 rows. The maximium is 10,000 rows.

```py
results = rd3.get(entity='<database>_<table>', batch_size=1000)
```

If you would like to filter data, use the parameter `q`. Filters can be defined using RSQL operators. This allows you to apply one or more filters using a number of different search types (e.g., `like`, `in`, etc.). Please see the [MOLGENIS RSQL Operators Guide](https://molgenis.gitbook.io/molgenis/interoperability/guide-rsql) for more information.

```py
results = rd3.get(entity='<database>_<table>', q="<column_name>==<some_value>", batch_size=1000)
```

Please see the [molgenis python client documentation](https://molgenis.gitbook.io/molgenis/interoperability/guide-client-python) for more information.

## Examples

In the following example, we will learn how to query tables and create filters.

### 1. How do I find SR-RNAseq samples submitted by ERN-RND?

This information is found in the samples table. We will need a few pieces of information to retrieve the metadata.

1. The entity name for the samples table
2. Name of the column where ERN affiliation is stored
3. The RD3 identifier for ERN-RND

In the [RD3 Table Schema](https://github.com/molgenis/molgenis-solve-rd/blob/main/model/schemas/rd3.md#entities), we can find the internal name of the samples table (Name: `samples`). Since we have to write the table name using the `<database>_<table>` format, we can find this information in the [RD3 Samples Table Schema](https://github.com/molgenis/molgenis-solve-rd/blob/main/model/schemas/rd3.md#entity-solverd_samples) (entity: `solverd_samples`).

Also in the [RD3 Samples Table Schema](https://github.com/molgenis/molgenis-solve-rd/blob/main/model/schemas/rd3.md#entity-solverd_samples), we can find the column for ERN affiliation (which is `ERN`). If you have a look at the last column, it shows that the data type for `ERN` is `xref` (i.e., cross reference). This means that you must know ID attached to each ERN record in the RD3 database. In RD3, this information is stored in the [ERN Lookup table](https://solve-rd.gcc.rug.nl/menu/data/dataexplorer?entity=solverd_info_erns&hideselect=true) (In RD3: Solve-RD >> Solve-RD Information >> European Reference Networks). In the table, find the record for "ERN-RND" and copy the value in the `id` column (`ern_rnd`).

Now that we have the entity name, column name, and the ID for ERN-RND, we can write the request.

```py
import molgenis.client as molgenis

rd3 = molgenis.Session('https://solve-rd.gcc.rug.nl/api/')
rd3.login(username='...', password='...')

data = rd3.get(entity='solverd_samples', q='ERN==ern_rnd', batch_size=1000)
```

For more information on the available fields in the samples table, please see the [samples table schema](https://github.com/molgenis/molgenis-solve-rd/blob/main/model/schemas/rd3.md#entity-solverd_samples).

### 2. How do I find special combinations of omics samples?

In some situtations, you would like to find combinations of samples. For example, which Epigenome samples were also included in LR-WGS or SR-WGS? It is possible to run searches like these, but we will need some information first.

1. The entity name for the samples table
2. Name of the column where sample type (i.e., omics) releases are stored
3. The RD3 identifiers for Epigenome, LR-WGS, and SR-WGS releases.

As described in the first example, the entity name for the samples table is `solverd_samples`.

WGS/WES and novel omics metadata is associated with a "release" in RD3. This allows us to find records across all tables in RD3 in the column `partOfRelease`. Find this column in the [RD3 Samples table schema](https://github.com/molgenis/molgenis-solve-rd/blob/main/model/schemas/rd3.md#entity-solverd_samples), and note the data type. The data type for `partOfRelease` is `mref` (or multi-reference) and a record can be associated with more than one release (e.g., WGS and SR-WGS). In order to filter data by release, you must know the internal ID associated with an RD3 release. This information is stored in the [RD3 Data Releases table](https://solve-rd.gcc.rug.nl/menu/data/dataexplorer?entity=solverd_info_datareleases&hideselect=true). Search for the Epigenome, LR-WGS, and SR-WGS releases, and copy the value in the `id` column.

Here's how to find the data releases table in the browser.

1. Sign in to RD3
2. In the "Data" tab, click the "View all Solve-RD Tables" link.
3. On the next page, click the folder "RD3 Information"
4. Lastly, click the link "Data Releases"

Since we would like to find records that are part of the Epigenome release *and* also part of the SR-WGS *or* LR-WGS releases, we will use an `AND` and `OR` search. The filter will contain two parts.

1. The first part will look for samples that are associated with the Epigenome release (`novelepigenome_original`): `partOfRelease==novelepigenome_original`.
2. The second part of the filter will look for records that are also part of the SR-WGS or LR-WGS releases. We will use an `in` search to look for both releases: `partOfRelease=in=(novelsrwgs_original,novellrwgs_original)`.

To indicate that we are running an `AND` search, separate the filters with a semicolon (`;`) and wrap the statement in parentheses.

```py
import molgenis.client as molgenis

rd3 = molgenis.Session('https://solve-rd.gcc.rug.nl/api/')
rd3.login(username='...', password='...')

data = rd3.get(
  entity='solverd_samples',
  q='(partOfRelease==novelepigenome_original;partOfRelease=in=(novelsrwgs_original,novellrwgs_original))',
  batch_size=1000
)
```

### 3. How do I find combined SR-WGS and SR-RNAseq samples?

In this example, we are looking for SR-WGS and SR-RNAseq samples. Since these are two samples, we need to conduct the search in the subjects table. This will show give us the subject IDs of those with SR-WGS and SR-RNAseq metadata, which we can use to retrieve the sample metadata.

1. Name of the subjects table
2. Name of the column where sample type (i.e., omics) releases are stored
3. The RD3 identifiers for the SR-WGS and SR-RNAseq releases

The entity name for the subjects table is `solverd_subjects`.

Like the previous examples, we will use the column `partOfRelease` to find subjects that have SR-WGS and SR-RNAseq metadata. Find these releases in the [RD3 Data Releases table](https://solve-rd.gcc.rug.nl/menu/data/dataexplorer?entity=solverd_info_datareleases&hideselect=true), and copy the value in the `id` column. In this example, these are `novelsrwgs_original` and `novelsrrnaseq_original`.

Sample metadata can be retrieved in two steps: 1) retrieve subject IDs that have SR-WGS and SR-RNAseq metadata and 2) retrieve samples using subject IDs found in step one.

```py
import molgenis.client as molgenis

rd3 = molgenis.Session('https://solve-rd.gcc.rug.nl/api/')
rd3.login(username='...', password='...')

# ~ 1 ~
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

# ~ 2 ~
# Retrieve sample metadata
# Using the subjectIDs extracted in step 1 and the release IDs, create a new
# filter. In the samples table, the reference to subject can be found in the
# column `belongsToSubject`

releaseQuery="partOfRelease=in=(novelsrrnaseq_original,novelsrwgs_original)"
subjectQuery="belongsToSubject=in=({subjectIDs})"
query = f"({releaseQuery};{subjectQuery})"

samples = rd3.get(entity='solver_samples', q=query, batch_size=1000)

```

### 4. How do I find subjects with SR-RNAseq and WES/WGS data?

In this example, we would like to find subjects have SR-RNAseq metadata and WES or WGS metadata. This information is stored in the subjects table. WES and WGS metadata is part of the main Solve-RD data freezes. Since there are a number of data freezes and updates, we will retrieve this information via the API and extract the IDs programmatically.

```py
import molgenis.client as molgenis

rd3 = molgenis.Session('https://solve-rd.gcc.rug.nl/api/')
rd3.login(username='...', password='...')

# ~ 1 ~
# Find release identifiers
# Releases are stored in the Data Releases table `solverd_info_datareleases`.
# All WES and WGS releases and updates are prefixed with "freeze". We can 
# extract the IDs by search for releases that are part of a freeze. We will
# also extract the SR-RNAseq release ID.

releases = rd3.get(entity='solverd_info_datareleases')

releaseIDs=[]
for row in releases:
  if 'Freeze' in row['name']:
    releaseIDs.append(row['id'])
  if 'SR-RNAseq' in row['name']:
    srRnaSeqId = row['id']

# ~ 2 ~
# Retrieve subjects
# Create an 'AND' search for subjects that have SR-RNAseq metadata and
# WES or WGS metadata

query=f"(partOfRelease=={srRnaSeqId};partOfRelease=({','.join(releaseIDs)}))"

subjects = rd3.get(entity='solverd_subjects', q=query, batch_size=1000)
```

### 5. How do I find files for subjects with SR-WGS and SR-RNAseq data?

```py
import molgenis.client as molgenis

rd3 = molgenis.Session('https://solve-rd.gcc.rug.nl/api/')
rd3.login(username='...', password='...')

# ~ 1 ~
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

# ~ 2 ~
# Retrieve files
# Using the subjectIDs extracted in step 1 and the release IDs, create a new
# filter. In the files table, the reference to subject can be found in the
# column `subjectID`

releaseQuery="partOfRelease=in=(novelsrrnaseq_original,novelsrwgs_original)"
subjectQuery="subjectID=in=({subjectIDs})"
query = f"({releaseQuery};{subjectQuery})"

files = rd3.get(entity='solverd_files', q=query, batch_size=10000)
```

## Further Information

- [RD3 Schema](https://github.com/molgenis/molgenis-solve-rd/blob/main/model/schemas/rd3.md)
- [Molgenis Python Client Docs](https://molgenis.gitbook.io/molgenis/interoperability/guide-client-python)
- [Molgenis Python Client GitHub](https://github.com/molgenis/molgenis-py-client)
