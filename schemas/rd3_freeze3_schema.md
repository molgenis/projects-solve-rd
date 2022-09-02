# Model Schema

## Packages

| Name | Description | Parent |
|:---- |:-----------|:------|
| rd3 | Solve-RD RD3 (v1.1.0, 2021-03-07) | - |
| rd3_freeze3 | RD3 Freeze 3 | rd3 |

## Entities

| Name | Description | Package |
|:---- |:-----------|:-------|
| subject | patients and family members within Freeze 3 | rd3_freeze3 |
| subjectinfo | Extra information about subjects within Freeze 3 | rd3_freeze3 |
| sample | Samples used as input for analyses within Freeze 3 | rd3_freeze3 |
| labinfo | information or process in the lab linked to samples | rd3_freeze3 |
| file | Individual files on file systems, files are linked in datasets, versioning with EGA accession number | rd3_freeze3 |
| job | Jobs used to process sampledata within Freeze 3 | rd3_freeze3 |

## Attributes

### Entity: rd3_freeze3_subject

patients and family members within Freeze 3

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | id | Unique identifier (subjectID + patch number) | string |
| subjectID&#8251; | SubjectID | Unique identifier of the subject | string |
| sex1&#8251; | Claimed Sex | Claimed Sex / Sex at birth (can be different from observed sex, see sample) | xref |
| fid&#8251; | FamilyID | Family ID (FID) | string |
| mid&#8251; | MaternalID | Maternal ID (MID) | xref |
| pid&#8251; | PaternalID | Paternal ID (PID) | xref |
| clinical_status&#8251; | Affected status | whether it is an affected individual (main disease) Yes/No | bool |
| disease&#8251; | disease | if affected, which MAIN disease code(s) apply to subject | mref |
| phenotype&#8251; | phenotype | Phenotype based on Human Phenotype Ontology (HPO) | mref |
| hasNotPhenotype&#8251; | hasNotPhenotype | Relevant Phenotype NOT present in subject | mref |
| phenopacketsID&#8251; | Phenotype Patch | name of the most recent phenopackets file (formated as <subjectID>.<date>.json) | string |
| patch&#8251; | Subject Releases | One or more RD3 releases associated with the subject | categorical_mref |
| variant&#8251; | variant | candidate pathogenic variant(s) for this subject | mref |
| organisation&#8251; | Organisation | Name of the organisation that submitted Subject | xref |
| ERN&#8251; | ERN | ERN | xref |
| solved&#8251; | solved | Solved Case for Solve-RD (true/false) | bool |
| date_solved&#8251; | DateSolved | Date Solved | date |
| remarks&#8251; | Remarks | Remarks about this Subject (Relevant for SOLVE-RD project) | text |
| consent&#8251; | Consent | - | compound |
| matchMakerPermission&#8251; | MatchMaker Permission | Permission is given for MatchMaking (boolean) | bool |
| noIncidentalFindings&#8251; | No Incidental Findings | Do NOT report incidental findings back (boolean) | bool |
| recontact&#8251; | Recontact Incidental findings | Recontact is allowed in case of incidental findings | categorical |
| retracted&#8251; | Retracted Subject | Is the subject retracted or not | categorical |
| patch_comment&#8251; | Release Comments | Patch Comment | string |

### Entity: rd3_freeze3_subjectinfo

Extra information about subjects within Freeze 3

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | id | Unique identifier (subjectID + patch number) | string |
| subjectID&#8251; | SubjectID | Unique identifier of the subject | string |
| sex1&#8251; | Claimed Sex | Claimed Sex / Sex at birth (can be different from observed sex, see sample) | xref |
| fid&#8251; | FamilyID | Family ID (FID) | string |
| mid&#8251; | MaternalID | Maternal ID (MID) | xref |
| pid&#8251; | PaternalID | Paternal ID (PID) | xref |
| clinical_status&#8251; | Affected status | whether it is an affected individual (main disease) Yes/No | bool |
| disease&#8251; | disease | if affected, which MAIN disease code(s) apply to subject | mref |
| phenotype&#8251; | phenotype | Phenotype based on Human Phenotype Ontology (HPO) | mref |
| hasNotPhenotype&#8251; | hasNotPhenotype | Relevant Phenotype NOT present in subject | mref |
| phenopacketsID&#8251; | Phenotype Patch | name of the most recent phenopackets file (formated as <subjectID>.<date>.json) | string |
| patch&#8251; | Subject Releases | One or more RD3 releases associated with the subject | categorical_mref |
| variant&#8251; | variant | candidate pathogenic variant(s) for this subject | mref |
| organisation&#8251; | Organisation | Name of the organisation that submitted Subject | xref |
| ERN&#8251; | ERN | ERN | xref |
| solved&#8251; | solved | Solved Case for Solve-RD (true/false) | bool |
| date_solved&#8251; | DateSolved | Date Solved | date |
| remarks&#8251; | Remarks | Remarks about this Subject (Relevant for SOLVE-RD project) | text |
| consent&#8251; | Consent | - | compound |
| matchMakerPermission&#8251; | MatchMaker Permission | Permission is given for MatchMaking (boolean) | bool |
| noIncidentalFindings&#8251; | No Incidental Findings | Do NOT report incidental findings back (boolean) | bool |
| recontact&#8251; | Recontact Incidental findings | Recontact is allowed in case of incidental findings | categorical |
| retracted&#8251; | Retracted Subject | Is the subject retracted or not | categorical |
| patch_comment&#8251; | Release Comments | Patch Comment | string |
| id&#8251; | id | Unique identifier (subjectID + patch number) | string |
| subjectID&#8251; | SubjectID | Unique identifier of the subject | xref |
| dateofBirth&#8251; | Date of Birth | Date of birth (YYYY) (COMMON DATA ELEMENTS 2.1) | int |
| ageOfDeath&#8251; | ageOfDeath | if deceased, age of death (COMMON DATA ELEMENTS 3.2) | int |
| ageOfOnset&#8251; | ageOfOnset | Age at onset (COMMON DATA ELEMENTS 5.1) | xref |
| ageOfDiagnosis&#8251; | ageOfDiagnosis | Age at diagnosis (COMMON DATA ELEMENTS 5.2) | int |
| Country_of_origin&#8251; | Country of Origin | country of origin (for pedigree?) | string |
| consanguinity_suspected&#8251; | Consanguinity suspected | subject consanguinity (yes,no) | bool |
| patch&#8251; | Subject Releases | One or more RD3 releases associated with the subject | categorical_mref |
| patch_comment&#8251; | Release Comment | Patch Comment | string |

### Entity: rd3_freeze3_sample

Samples used as input for analyses within Freeze 3

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | id | unique identifier (sampleID + patch) | string |
| sampleID&#8251; | sampleID | Identifier of the sample | string |
| alternativeIdentifier&#8251; | alternativeIdentifier | alternative identifier used by sample provider | string |
| subject&#8251; | subject | reference to the subject from which samples was extracted | xref |
| ageAtSampling&#8251; | ageAtSampling | age at sampling/ First contact with specialised centre (COMMON DATA ELEMENTS 4.1) | int |
| sex1&#8251; | Claimed sex | Claimed Sex / Sex at birth (can be different from observed sex, see sample) | categorical |
| sex2&#8251; | Observed sex | Computed sex (based on Y- chromosome) | categorical |
| pathologicalState&#8251; | - | The pathological state of the tissue from which this material was derived. (GO:0001894) | xref |
| percentageTumorCells&#8251; | - | The percentage of tumor cells compared to total cells present in this material. (NCIT:C127771) | decimal |
| tissueType&#8251; | Tissue Types | - | xref |
| extractedProtocol&#8251; | Extracted Protocol | - | string |
| materialType&#8251; | materialType | material type. E.g. DNA | mref |
| flag&#8251; | QC failed | QC failed (True /False) | bool |
| organisation&#8251; | organisation | Name of the organisation. E.g. University Medical Center Groningen | xref |
| ERN&#8251; | ERN | ERN | xref |
| retracted&#8251; | Retracted Sample | - | categorical |
| dateAvailable&#8251; | Date available | Date that a file became visible to sandbox/RD3 | datetime |
| anatomicalLocation&#8251; | anatomicalLocation | - | xref |
| batch&#8251; | Batch Number | Sample batch number | string |
| patch&#8251; | Sample Releases | One or more RD3 releases associated with the sample | categorical_mref |
| patch_comment&#8251; | Release Comment | Patch Comment | string |

### Entity: rd3_freeze3_labinfo

information or process in the lab linked to samples

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | id | Unique identifier (experimentID + patch) | string |
| experimentID&#8251; | experimentID | Experiment ID | string |
| sample&#8251; | sample | all samples run in this condition, using the same barcode | mref |
| capture&#8251; | Enrichment kit | Enrichment kit | string |
| libraryType&#8251; | Library Source | Library Source, e.g Genomic/Transcriptomic | categorical |
| flowcell&#8251; | flowcell | flowcell information | string |
| barcode&#8251; | barcode | barcode involved | string |
| samplePosition&#8251; | samplePosition | lane, or possition in well (A1 t/mH12) | string |
| library&#8251; | library | link to more information about the library used in experiment | mref |
| sequencingCentre&#8251; | Sequencing Centre | Centre where samples were sequenced | string |
| sequencer&#8251; | sequencer | sequencerinfo | string |
| seqType&#8251; | seqType | sequencing technique types (e.g. WXS, WGS) | xref |
| arrayID&#8251; | arrayID | arrayID | string |
| mean_cov&#8251; | MeanCov | Mean read depth | decimal |
| c20&#8251; | C20 | Percentage of nucleotides within the Region of interest that is covered by at least 20 reads | decimal |
| retracted&#8251; | Retracted Experiment | Is the experiment retracted or not | categorical |
| patch&#8251; | Experiment Releases | One or more RD3 releases associated with the sample | categorical_mref |
| patch_comment&#8251; | Release Comment | Patch Comment | string |

### Entity: rd3_freeze3_file

Individual files on file systems, files are linked in datasets, versioning with EGA accession number

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| EGA&#8251; | EGA Accession Number | EGA Accession Number | string |
| name&#8251; | Filename | Filename | string |
| md5&#8251; | Checksum | hash code to check if file is not corrupted. | string |
| typeFile&#8251; | typeFile | type of file (BAM, FastQ, gVCF, phenopacket, BED, etc.) | categorical |
| samples&#8251; | samples | link to file(s) produced for this sample | mref |
| subjectID&#8251; | SubjectID | Unique identifier of the subject | mref |
| patch&#8251; | File Releases | One or more RD3 releases associated with the file | categorical_mref |
| patch_comment&#8251; | Patch Comment | Patch Comment | string |
| dateCreated&#8251; | Created | Creation date in RD3 database | date |
| extraInfo&#8251; | Extra Information | - | text |
| experimentID&#8251; | experimentID | Identifier of the corresponding record in the labinfo table | string |
| filepath_sandbox&#8251; | Filepath Sandbox | path of the file stored in the sandbox environment | text |

### Entity: rd3_freeze3_job

Jobs used to process sampledata within Freeze 3

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| status&#8251; | status | status of job (running, completed, or error) | enum |
| identifier&#8251; | identifier | unique identifier job | string |
| run&#8251; | run | job is part of run | xref |
| log&#8251; | log | uri referencing to logfile | string |
| inputfile&#8251; | inputfile | inputfile, link to file used in job | mref |
| outputfile&#8251; | outputfile | file produced with job | mref |
| jobstart&#8251; | jobstart | start date and time of job | datetime |
| jobend&#8251; | jobend | end date and time of job | datetime |

Note: The symbol &#8251; denotes attributes that are primary keys

