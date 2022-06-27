# Model Schema

## Packages

| Name | Description | Parent |
|:---- |:-----------|:------|
| rd3stats | Additional summaries and processed data (v1.1.0, 2022-06-27) | - |

## Entities

| Name | Description | Package |
|:---- |:-----------|:-------|
| treedata | JSON stringified objects of sample-experiment links per subject | rd3stats |

## Attributes

### Entity: rd3stats_treedata

JSON stringified objects of sample-experiment links per subject

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | identifier of the object | string |
| subjectID | - | An individual who is the subject of personal data, persons to whom data refers, and from whom data are collected, processed, and stored. | string |
| familyID | - | A domestic group, or a number of domestic groups linked through descent (demonstrated or stipulated) from a common ancestor, marriage, or adoption. | string |
| json | - | json stringified object containing sample-experiment links | text |

Note: The symbol &#8251; denotes attributes that are primary keys

