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
| genderAtBirth | - | Assigned gender is one's gender which was assigned at birth, typically by a medical and/or legal organization, and then later registered with other organizations. Such a designation is typically based off of the superficial appearance of external genitalia present at birth. | mref |
| belongsToFamily | - | A domestic group, or a number of domestic groups linked through descent (demonstrated or stipulated) from a common ancestor, marriage, or adoption. | string |
| belongsToMother | - | A designation that has some relationship to motherhood. | xref |
| belongsToFather | - | Having to do with the father, coming from the father, or related through the father. | xref |
| partOfDataRelease | - | The act of making data or other structured information accessible to the public or to the user group of a database. | mref |
| primaryAffiliatedInstitute | - | The most significant institute for medical consultation and/or study inclusion in context of the genetic disease of this person. | mref |
| participatesInStudy | - | Reference to the study or studies in which this person participates. | mref |
| affectedStatus | - | Individuals in a pedigree who exhibit the specific phenotype under study. | bool |
| solvedStatus | - | Solved status for RD3 (True/False) | bool |
| observedPhenotype | - | The outward appearance of the individual. In medical context, these are often the symptoms caused by a disease. | mref |
| unobservedPhenotype | - | Phenotypes or symptoms that were looked for but not observed, which may help in differential diagnosis or establish incomplete penetrance. | mref |
| clinicalDiagnosis | - | A diagnosis made from a study of the signs and symptoms of a disease. | mref |
| emxRelease | - | identifier for nested EMX package | string |
| samples | - | - | compound |
| numberOfSamples | - | - | int |
| df1Samples | - | - | mref |
| df2Samples | - | - | mref |
| df3Samples | - | - | mref |
| noveldeepwesSamples | - | - | mref |
| novelrnaseqSamples | - | - | mref |
| novelsrwgsSamples | - | - | mref |
| novelwgsSamples | - | - | mref |
| experiments | - | - | compound |
| numberOfExperiments | - | - | int |
| df1Experiments | - | - | mref |
| df2Experiments | - | - | mref |
| df3Experiments | - | - | mref |
| noveldeepweesExperiments | - | - | mref |
| novelrnaseqExperiments | - | - | mref |
| novelsrwgsExperiments | - | - | mref |
| novelwgsExperiments | - | - | mref |
| files | - | - | compound |
| numberOfFiles | - | - | int |
| df1Files | - | - | mref |
| df2Files | - | - | hyperlink |
| df3Files | - | - | hyperlink |
| noveldeepwesFiles | - | - | hyperlink |
| novelrnaseqFiles | - | - | hyperlink |
| novelsrwgsFile | - | - | hyperlink |
| novelwgsFile | - | - | hyperlink |

Note: The symbol &#8251; denotes attributes that are primary keys

