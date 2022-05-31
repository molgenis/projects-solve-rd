# Model Schema

## Packages

| Name | Description | Parent |
|:---- |:-----------|:------|
| rd3 | Solve-RD RD3 (v1.1.0, 2021-03-07) | - |

## Entities

| Name | Description | Package |
|:---- |:-----------|:-------|
| overview | Overview on RD3 Subjects, samples, and experiments | rd3 |

## Attributes

### Entity: rd3_overview

Overview on RD3 Subjects, samples, and experiments

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| subjectID&#8251; | - | A unique proper name or character sequence that identifies this particular individual who is the subject of personal data, persons to whom data refers, and from whom data are collected, processed, and stored. | string |
| sex1 | Claimed Sex | Assigned gender is one's gender which was assigned at birth, typically by a medical and/or legal organization, and then later registered with other organizations. Such a designation is typically based off of the superficial appearance of external genitalia present at birth. | mref |
| fid | FamilyID | A domestic group, or a number of domestic groups linked through descent (demonstrated or stipulated) from a common ancestor, marriage, or adoption. | string |
| mid | MaternalID | A designation that has some relationship to motherhood. | xref |
| pid | PaternalID | Having to do with the father, coming from the father, or related through the father. | xref |
| patch | Patch | The act of making data or other structured information accessible to the public or to the user group of a database. | mref |
| organisation | Organisation | The most significant institute for medical consultation and/or study inclusion in context of the genetic disease of this person. | mref |
| ERN | ERN | Reference to the study or studies in which this person participates. | mref |
| affectedStatus | Affected Status | Individuals in a pedigree who exhibit the specific phenotype under study. | bool |
| solved | - | Solved status for RD3 (True/False) | bool |
| phenotype | - | The outward appearance of the individual. In medical context, these are often the symptoms caused by a disease. | mref |
| hasNotPhenotype | None | Phenotypes or symptoms that were looked for but not observed, which may help in differential diagnosis or establish incomplete penetrance. | mref |
| disease | - | A diagnosis made from a study of the signs and symptoms of a disease. | mref |
| samples | - | - | compound |
| numberOfSamples | Total Number of Samples | - | int |
| df1Samples | DF1 Samples | - | mref |
| df2Samples | DF2 Samples | - | mref |
| df3Samples | DF3 Samples | - | mref |
| noveldeepwesSamples | NovelOmics Deep-WES Samples | - | mref |
| novelrnaseqSamples | NovelOmics RNAseq Samples | - | mref |
| novellrwgsSamples | NovelOmics LR-WGS Samples | - | mref |
| novelsrwgsSamples | NovelOmics SR-WGS Samples | - | mref |
| novelwgsSamples | NovelOmics WGS Samples | - | mref |
| experiments | - | - | compound |
| numberOfExperiments | Total Number of Experiments | - | int |
| df1Experiments | DF1 Experiments | - | mref |
| df2Experiments | DF2 Experiments | - | mref |
| df3Experiments | DF1 Experiments | - | mref |
| noveldeepweesExperiments | NovelOmics Deep-WES Experiments | - | mref |
| novelrnaseqExperiments | NovelOmics RNAseq Experiments | - | mref |
| novelsrwgsExperiments | NovelOmics SR-WGS Experiments | - | mref |
| novelwgsExperiments | NovelOmics WGS Experiments | - | mref |
| files | - | - | compound |
| numberOfFiles | Total Number of Files | - | int |
| df1Files | DF1 Files | - | hyperlink |
| df2Files | DF2 Files | - | hyperlink |
| df3Files | DF3 Files | - | hyperlink |
| noveldeepwesFiles | NovelOmics Deep-WES Files | - | hyperlink |
| novelrnaseqFiles | NovelOmics RNAseq Files | - | hyperlink |
| novelsrwgsFile | NovelOmics SR-WGS Files | - | hyperlink |
| novelwgsFile | NovelOmics WGS Files | - | hyperlink |
| associatedRD3Releases | - | identifier for nested EMX package | string |

Note: The symbol &#8251; denotes attributes that are primary keys

