# RD3 Data Management

In this folder you will find a number of scripts that are used to manage the RD3 database, as well as various tools and API clients. This document will provide an overview of the main scripts and provide information on how to run them.

## Getting Started

Many scripts, if not all, require an account to run and credentials are stored in an `.env` file. If you do not have an RD3 (local molgenis user) account, please request one. When you an account, create a `.env` file in the main folder (`molgenis-solve-rd`). In the file, you should have the following variables defined.

```txt
# hosts
MOLGENIS_ACC_HOST=...
MOLGENIS_PROD_HOST=...

# user credentials per host
MOLGENIS_PROD_USR="..."
MOLGENIS_PROD_PWD="..."

MOLGENIS_ACC_USR="..."
MOLGENIS_ACC_PWD="..."
```

The `.env` file is loaded into a script using function `load_dotenv()` from the `dotenv` library.
