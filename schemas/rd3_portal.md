# Model Schema

## Packages

| Name | Description | Parent |
|:---- |:-----------|:------|
| rd3_portal | RD3 portal, containing data submitted by CNAG (v1.1.0, 2021-10-11) | - |
| rd3_portal_cluster | Extracted metadata PED and Phenopacket files stored on the cluster (v1.0.0, 2022-08-08) | rd3_portal |

## Entities

| Name | Description | Package |
|:---- |:-----------|:-------|
| phenopacket | Extracted data from Phenopacket files | rd3_portal_cluster |
| ped | Extracted data from PED files | rd3_portal_cluster |

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

### Entity: rd3_portal_cluster_ped

Extracted data from PED files

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| rowID&#8251; | - | an auto generated ID | string |
| pedID | - | Name of the Phenopacket file | string |
| clusterRelease | - | the release, on the cluster, where the data comes from | string |
| subjectID | - | RD3 subject identifier | string |
| fid | - | family identifier | string |
| mid | - | identifier of the mother | string |
| pid | - | identifier of the father | string |
| sex1 | - | If available, a string containing the sex of the patient | string |
| clinical_status | - | An indication of affected status | bool |
| unknownMID | - | The identifier of the mother that was reported. It was either invalid or could not be found in RD3 | string |
| unknownPID | - | The identifier of the father that was reported. It was either invalid or could not be found in RD3 | string |
| unknownSexCodes | - | Code that was detected in the PED file, but it does not match expected values (1,2, other) | string |
| unknownClinicalStatus | - | A code that was detected in the PED file, but it does not match an expected value (-9, 0, 1,2) | string |
| subjectExists | - | An indication if the subject exists in RD3 | bool |
| releasesWhereSubjectExists | - | If the subject exists, which release | string |
| upload | - | If True, the raw data for this subject can be imported into RD3 | string |
| processed | - | An indication if the data has been imported into RD3 somewhere | bool |

Note: The symbol &#8251; denotes attributes that are primary keys

