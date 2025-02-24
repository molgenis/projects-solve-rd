# Projects RD GDI

The purpose of this repository is to store all scripts and code related to the MOLGENIS-GDI portal.

## Getting started

To rebuild the example datasets, you will need to request access to the cluster. Once this is done, you can run the scripts.

### Setting variables

You will need to create `.env` file and enter the following values

```text
ENTRY_DIR="..."
OUTPUT_DIR="..."

MOLGENIS_HOST="..."
MOLGENIS_USR="..."
MOLGENIS_TOKEN="..."
```

| Name | Description |
|:-----|:------------|
| ENTRY_DIR | entry point to look for files on the cluster |
| OUTPUT_DIR | location to write datasets (on the cluster) |
| MOLGENIS_HOST | URL of your local molgenis instance |
| MOLGENIS_USR | your molgenis account |
| MOLGENIS_TOKEN | a unique token that allows you to connect to molgenis via the py client |
