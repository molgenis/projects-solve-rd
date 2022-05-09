# Model Schema

## Packages

| Name | Description | Parent |
|:---- |:-----------|:------|
| rd3_portal | RD3 portal, containing data submitted by CNAG (v1.1.0, 2021-10-11) | - |
| rd3_portal_release | Intermediate tables for RD3 releases | rd3_portal |
| rd3_portal | RD3 portal, containing data submitted by CNAG | - |
| rd3_portal_novelomics | Staging tables for novel omics sample and experiment metadata (v1.4.0, 2022-02-03) | rd3_portal |
| rd3_portal_release | Intermediate tables for RD3 releases | rd3_portal |

## Entities

| Name | Description | Package |
|:---- |:-----------|:-------|
| cluster | Metadata on PED and Phenopacket files located on the cluster | rd3_portal |
| demographics | Patient demographics submitted by CNAG | rd3_portal |
| experiment | Staging table for experiment metadata (manifest file) | rd3_portal_novelomics |
| shipment | Staging table for sample metadata | rd3_portal_novelomics |
| attrTmplate | Attribute template for new RD3 Data Freezes | rd3_portal_release |
| freeze2 | Staging table for raw DF2 data | rd3_portal_release |
| freeze3 | Staging table for raw DF3 data | rd3_portal_release |
| novelwgs | Staging table for raw Novel Omics WGS data | rd3_portal_release |

## Attributes

### Entity: rd3_portal_cluster

Metadata on PED and Phenopacket files located on the cluster

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| release | release | Patch information | string |
| path | path | path to location of the file | string |
| name | name | file name | string |
| type | type | file type | string |
| md5sum | md5sum | file checksum | string |
| created | created | Date entry was created | datetime |
| id&#8251; | id | - | string |

### Entity: rd3_portal_demographics

Patient demographics submitted by CNAG

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| subjectID | subjectID | RD3 patient ID | string |
| experimentID | ExperimentID | experiment ID | string |
| sex | Observed Sex | Observed Sex | string |
| ethnicity | Calculated Ancestry | Ancestry that was derived | string |
| auto_id&#8251; | auto_id | auto generated molgenis ID | string |

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
| processed | - | - | bool |
| molgenis_id&#8251; | - | - | string |

### Entity: rd3_portal_novelomics_shipment

Staging table for sample metadata

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| sample_id | - | - | string |
| participant_subject | - | - | string |
| type_of_analysis | - | - | string |
| tissue_type | - | - | string |
| sample_type | - | - | string |
| solve_rd_experiment_id | - | - | string |
| batch | - | - | string |
| organisation | - | - | string |
| ERN | - | - | string |
| CNAG_barcode | - | - | string |
| alternative_sample_identifier | - | - | string |
| date_created | - | Date the data was uploaded into RD3 | datetime |
| processed | - | - | bool |
| molgenis_id&#8251; | - | - | string |

### Entity: rd3_portal_release_attrTmplate

Attribute template for new RD3 Data Freezes

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | auto generated molgenis ID | string |
| organisation_name | - | name of the organisation. E.g. University Medical Center Groningen | string |
| organisation_identifier | - | identifier of the organisation. E.g. UMCG¬† | string |
| samples_tissueType | - | Tissue Types | string |
| samples_id | - | unique identifier (sampleID + patch) | string |
| samples_alternativeIdentifier | - | alternative identifier used by sample provider | string |
| samples_subject | - | reference to the subject from which samples was extracted | string |
| samples_organisation | - | Name of the organisation. E.g. University Medical Center Groningen | string |
| samples_ERN | - | ERN | string |
| labinfo_library | - | link to more information about the library used in experiment | string |
| labinfo_sequencer | - | sequencer info | string |
| labinfo_seqType | - | sequencing technique types (e.g., WGS, WXS) | string |
| labinfo_id | - | Unique identifier (experimentID + patch) | string |
| labinfo_sample | - | all samples run in this condition, using the same barcode | string |
| labinfo_capture | - | enrichment kit | string |
| labinfo_libraryType | - | library source, e.g., Genomic/Transcriptomic | string |
| subject_id | - | Unique identifier (subjectID + patch) | string |
| subject_organisation | - | name of the organisation that submitted the subject | string |
| subject_ERN | - | ERN | string |
| subject_solved | - | solved case for solve-RD (true/false) | string |
| subject_date_solved | - | Date case was solved | string |
| subject_matchMakerPermission | - | permission is given for match making (boolean) | string |
| subject_recontact | - | Recontact is allowed in case of incidental findings | string |
| processed | - | If True, the record has been processed | bool |

Note: The symbol &#8251; denotes attributes that are primary keys

