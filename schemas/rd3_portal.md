# Model Schema

## Packages

| Name | Description | Parent |
|:---- |:-----------|:------|
| rd3_portal | RD3 portal, containing data submitted by CNAG (v1.1.0, 2021-10-11) | - |
| rd3_portal_novelomics | Staging tables for novel omics sample and experiment metadata (v1.5.0, 2022-05-30) | rd3_portal |

## Entities

| Name | Description | Package |
|:---- |:-----------|:-------|
| experiment | Staging table for experiment metadata (manifest file) | rd3_portal_novelomics |
| shipment | Staging table for sample metadata | rd3_portal_novelomics |

## Attributes

### Entity: rd3_portal_novelomics_experiment

Staging table for experiment metadata (manifest file)

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| file_path | - | - | string |
| file_name | - | - | string |
| unencrypted_md5_checksum | - | - | string |
| encrypted_md5_checksum | - | - | string |
| file_type | - | - | string |
| file_size | - | - | string |
| file_group_id | - | - | string |
| batch_id | - | - | string |
| project_experiment_dataset_id | - | - | string |
| sample_id | - | - | string |
| sample_links | - | - | string |
| gender | - | - | string |
| subject_id | - | - | string |
| phenotype | - | - | string |
| subject_links | - | - | string |
| sequencing_center | - | - | string |
| platform_brand | - | - | string |
| platform_model | - | - | string |
| library_source | - | - | string |
| library_selection | - | - | string |
| library_strategy | - | - | string |
| library_layout | - | - | string |
| paired_nominal_length | - | - | string |
| paired_nominal_sdev | - | - | string |
| experiment_links | - | - | string |
| analysis_center | - | - | string |
| analysis_type | - | - | string |
| sequencing_type | - | - | string |
| reference_genome | - | - | string |
| linked_object | - | - | string |
| platform | - | - | string |
| program | - | - | string |
| imputation | - | - | string |
| analysis_links | - | - | string |
| experiment_type | - | - | string |
| tissue_type | - | - | string |
| sample_type | - | - | string |
| sub_project_name | - | - | string |
| project_batch_id | - | - | string |
| file_ega_id | - | - | string |
| run_ega_id | - | - | string |
| experiment_ega_id | - | - | string |
| sample_ega_id | - | - | string |
| date_created | - | Date the data was uploaded into RD3 | datetime |
| processed | - | indication if the the record has been processed into an RD3 release | bool |
| molgenis_id&#8251; | - | an internal, auto generated identifier | string |

### Entity: rd3_portal_novelomics_shipment

Staging table for sample metadata

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| sample_id | - | - | string |
| participant_subject | - | - | string |
| type_of_analysis | - | The analysis that was performed; e.g., SR-WGS, Deep-WES, RNAseq, etc. | string |
| tissue_type | - | - | string |
| sample_type | - | - | string |
| solve_rd_experiment_id | - | corresponding experiment identifier | string |
| batch | - | - | string |
| organisation | - | the submitting institution and submitter initials | string |
| ERN | - | associated ERN | string |
| CNAG_barcode | - | - | string |
| alternative_sample_identifier | - | alternative identifiers associated with the sample | string |
| pathological_state | - | affected state of the material | string |
| tumor_cell_fraction | - | the percentange of tumor cells compared to total cells present in this material | string |
| date_created | - | Date the data was uploaded into RD3 | datetime |
| processed | - | indication if the the record has been processed into an RD3 release | bool |
| molgenis_id&#8251; | - | an internal, auto generated identifier | string |

Note: The symbol &#8251; denotes attributes that are primary keys

