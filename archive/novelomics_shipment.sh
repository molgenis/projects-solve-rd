# ////////////////////////////////////////////////////////////////////////////
# FILE: novelomics_experiment.sh
# AUTHOR: David Ruvolo
# CREATED: 2021-03-09
# MODIFIED: 2021-03-09
# PURPOSE: import post-processing Novel Omics Experiment Table into RD3
# DEPENDENCIES: mcmd
# COMMENTS:
#   - mcmd: https://github.com/molgenis/molgenis-tools-commander
#   - R: R/novelomics_srwgs.R
# ////////////////////////////////////////////////////////////////////////////

# install
# pip install molgenis-commander
# pip install --upgrade molgenis-commander

# config
# mcmd config add host  # https://solve-rd-acc.gcc.rug.nl/
# mcmd config add host  # https://solve-rd.gcc.rug.nl/
mcmd config set host
mcmd import -p data/novelomics-shipment/novelomics-shipment_attributes.csv --as attributes --in rd3