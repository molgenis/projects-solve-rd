"""
Python script to download solve-RD data in batches, from the server.

"""

# imports
from os import environ
from dotenv import load_dotenv
import molgenis.client
import pandas as pd
load_dotenv()
import json

# connect to the RD3 EMX1 environment and log in
rd3 = molgenis.client.Session(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# ///////////////////////////////////////////////////////////////////////////////
# Get the Samples (solverd_samples) data. 

# initialize list to gather all samples
solveRD_samples = []

# define the number of rows (samples) in the table
num_rows = 27846

# define the batch size
step = 1000

# loop through the batches and gather the rows (samples) for each batch
for batch in range(0, num_rows, step):
    samples = rd3.get('solverd_samples', start=batch, num=step)
    solveRD_samples.extend(samples)

# write the dictionary to a json file
with open('samples.json', 'w') as file:
    json.dump(solveRD_samples, file)

# ///////////////////////////////////////////////////////////////////////////////
# Get the Experiments (solverd_labinfo) data.

# TO DO

# ///////////////////////////////////////////////////////////////////////////////
# Get the Files (solverd_files) data.

# TO DO