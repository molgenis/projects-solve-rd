# Model Schema

## Packages

| Name | Description | Parent |
|:---- |:-----------|:------|
| rd3_portal | RD3 portal, containing data submitted by CNAG (v1.1.0, 2021-10-11) | - |
| rd3_portal_cluster | Extracted metadata PED and Phenopacket files stored on the cluster (v0.9.2, 2022-08-02) | rd3_portal |

## Entities

| Name | Description | Package |
|:---- |:-----------|:-------|
| phenopacket | Extracted data from Phenopacket files | rd3_portal_cluster |

## Attributes

### Entity: rd3_portal_cluster_phenopacket

Extracted data from Phenopacket files

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| phenopacketsID&#8251; | - | Name of the Phenopacket file | string |
| clusterRelease | - | the release, on the cluster, where the data comes from | string |
| subjectID | - | RD3 subject identifier | string |
| dateofBirth | - | If available, the date the patient was born | string |
| sex1 | - | If available, a string containing the sex of the patient | string |
| phenotype | - | If available, a comma separated strings containing HPO codes | text |
| hasNotPhenotype | - | If available, a comma separated strings containing HPO codes | text |
| disease | - | If available, a comma separated strings containing disease codes | text |
| ageOfOnset | - | If available, the age of the patient at onsent | string |
| subjectExists | - | An indication if the subject exists in RD3 | bool |
| releasesWhereSubjectExists | - | If the subject exists, which release | string |
| unknownHpoCodes | - | A comma-separated string of all unknown HPO codes | text |
| unknownDiseaseCodes | - | A comma-separated string of all unknown disease codes | text |
| unknownOnsetCodes | - | A comma-separated string of all unknown onset codes | text |
| processed | - | An indication if the data has been imported into RD3 somewhere | bool |

Note: The symbol &#8251; denotes attributes that are primary keys

