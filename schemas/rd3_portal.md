# Model Schema

## Packages

| Name | Description | Parent |
|:---- |:-----------|:------|
| rd3_portal | RD3 portal, containing data submitted by CNAG (v1.1.0, 2021-10-11) | - |
| cluster | Extracted metadata PED and Phenopacket files stored on the cluster (v0.9.0, 2022-08-01) | - |

## Entities

| Name | Description | Package |
|:---- |:-----------|:-------|
| phenopacket | Extracted data from Phenopacket files | cluster |

## Attributes

### Entity: cluster_phenopacket

Extracted data from Phenopacket files

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| phenopacketsID&#8251; | - | Name of the Phenopacket file | string |
| subjectID | - | RD3 subject identifier | string |
| dateofBirth | - | If available, the date the patient was born | string |
| sex1 | - | If available, a string containing the sex of the patient | string |
| phenotype | - | If available, a comma separated strings containing HPO codes | string |
| hasNotPhenotype | - | If available, a comma separated strings containing HPO codes | string |
| disease | - | If available, a comma separated strings containing disease codes | string |
| ageOfOnset | - | If available, the age of the patient at onsent | string |
| subjectIdExists | - | An indication if the subject exists in RD3 | bool |
| unknownHpoCodes | - | A comma-separated string of all unknown HPO codes | string |
| unknownDiseaseCodes | - | A comma-separated string of all unknown disease codes | string |
| unknownOnsetCodes | - | A comma-separated string of all unknown onset codes | string |

Note: The symbol &#8251; denotes attributes that are primary keys

