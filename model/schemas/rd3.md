# Model Schema

## Packages

| Name | Description | Parent |
|:---- |:-----------|:------|
| solverd | Solve-RD RD3 database (v2.1.0, 2024-03-18) | - |
| solverd_info | Information about RD3 Releases, Studies, Cohorts, Collaborators, etc. | solverd |
| solverd_lookups | Lookup tables that contain standarized terms, codes, definitions, etc. | solverd |
| solverd_variants | - | solverd |

## Entities

| Name | Description | Package |
|:---- |:-----------|:-------|
| subjects | Information about patients and family members withing RD3 | solverd |
| subjectinfo | Additional information about subjects included in RD3 | solverd |
| samples | Samples used as input for analyses in RD3 | solverd |
| labinfo | Information or proceesses in the lab that are linked to samples | solverd |
| files | Individuals files stored in the sandbox environments. Files are linked to subjects, samples, and experiments, as well as with EGA accession number. | solverd |
| overview | Overview on RD3 Subjects, samples, and experiments | solverd |
| cohorts | A group of individuals, identified by a common characteristic. | solverd_info |
| datareleases | The act of making data or other structured information accessible to the public or to the user group of a database. | solverd_info |
| datasets | container of files belonging together (different datasets can contain overlapping files, except for the intial datasets containing startfiles (before analysis, startfiles= BAM, gVCF files (one per chromosome) and phenopacket file)) | solverd_info |
| erns | European Reference Networks, source: https://ec.europa.eu/health/ern/networks_en | solverd_info |
| jobs | Jobs used to process sample data | solverd_info |
| organisations | Organisation information standardized to the Research Organisation Registry (ROR) | solverd_info |
| persons | researcher or contactperson involved in the study and/or affliated with organisation | solverd_info |
| publications | Publication linked to subject or variants | solverd_info |
| run | container of jobs | solverd_info |
| studies | A detailed examination, analysis, or critical inspection of one or multiple subjects designed to discover facts. | solverd_info |
| ageGroups | How long something has existed; elapsed time since birth. | solverd_lookups |
| anatomicallocation | SNOMED CT, code list for anatomicalLocation (Snomed, Body Structure ontlogy used) See: http://bioportal.bioontology.org/ontologies/SNMD_BDY | solverd_lookups |
| country | - | solverd_lookups |
| dataUseConditions | code list describing different types of conditions to access the data | solverd_lookups |
| disease | ORDO codes en MIM codes | solverd_lookups |
| library | sequencing library information | solverd_lookups |
| libraryType | Library Source, e.g Genomic/Transcriptomic | solverd_lookups |
| materialType | code list for materialType according to MIABIS-2.0-14, e.g. DNA | solverd_lookups |
| noyesunknown | Categories No, Unknown, Yes | solverd_lookups |
| pathologicalstate | None | solverd_lookups |
| phenotype | HPO code list for phenotype, version 2020-02-27 http://bioportal.bioontology.org/ontologies/HP | solverd_lookups |
| seqType | Sequencing technique types (in CNAG batchfile = ExpType) | solverd_lookups |
| sex | code list for sex. According to MIABIS-2.0-09, E.g. 'M' | solverd_lookups |
| tissueType | TissueTypes, source is GTeX;  https://www.gtexportal.org/home/tissueSummaryPage | solverd_lookups |
| typeFile | type of x (e.g. BAM, gVCF, phenopacket, BED, etc.) | solverd_lookups |
| assembly | Human reference sequence used (https://genome-euro.ucsc.edu/cgi-bin/hgGateway?redirect=manual&source=genome.ucsc.edu) | solverd_variants |
| classification | clinical classification (https://blueprintgenetics.com/variant-classification/) | solverd_variants |
| variant | variant information according toHGVS (http://varnomen.hgvs.org/) | solverd_variants |
| variantType | Sequence variant types (https://varnomen.hgvs.org/recommendations/DNA) | solverd_variants |
| variantinfo | variantinfo specific per subject | solverd_variants |

## Attributes

### Entity: solverd_subjects

Information about patients and family members withing RD3

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| subjectID&#8251; | - | An individual who is the subject of personal data, persons to whom data refers, and from whom data are collected, processed, and stored. | string |
| sex1 | Claimed sex | Claimed Sex / Sex at birth (can be different from observed sex, see sample) | xref |
| fid | FamilyID | A domestic group, or a number of domestic groups linked through descent (demonstrated or stipulated) from a common ancestor, marriage, or adoption. | string |
| mid | MaternalID | Identifier indicating a reference to mother of the subject | xref |
| pid | PaternalID | Identifier indicating a reference to father of the subject | xref |
| clinical_status | Affected status | whether it is an affected individual (main disease) Yes/No | bool |
| disease | disease | if affected, which MAIN disease code(s) apply to subject | mref |
| phenotype | phenotype | Phenotype based on Human Phenotype Ontology (HPO) | mref |
| hasNotPhenotype | hasNotPhenotype | Relevant Phenotype NOT present in subject | mref |
| mostRecentPhenopacketFile | - | name of the most recent phenopackets file (formated as <subjectID>.<date>.json) | string |
| partOfRelease | - | One or more Solve-RD releases that indicate when the record was first introduced into RD3 or when it was updated. | mref |
| includedInStudies | - | Reference to the study or studies in which this person participates. | mref |
| includedInCohorts | - | A group of individuals, identified by a common characteristic. | mref |
| includedInDatasets | - | Reference to the dataset or datasets in which this person was assigned to. | mref |
| variant | variant | candidate pathogenic variant(s) for this subject | mref |
| organisation | Organisation | Name of the organisation that submitted Subject | mref |
| ERN | ERN | ERN | mref |
| solved | solved | Solved Case for Solve-RD (true/false) | bool |
| date_solved | DateSolved | Date Solved | date |
| remarks | Remarks | Remarks about this Subject (Relevant for SOLVE-RD project) | text |
| consent | Consent | - | compound |
| matchMakerPermission | MatchMaker Permission | If true, permission is given for MatchMaking (boolean) | bool |
| noIncidentalFindings | No Incidental Findings | If true, do NOT report incidental findings back (boolean) | bool |
| recontact | Recontact Incidental findings | Recontact is allowed in case of incidental findings | categorical |
| retracted | Retracted Subject | Is the subject retracted or not | categorical |
| recordMetadata | - | metadata is data that provides information about data. | compound |
| dateRecordCreated | - | The date on which the activity or entity is created. | date |
| recordCreatedBy | - | Indicates the person or authoritative body who brought the item into existence. | xref |
| dateRecordUpdated | - | The date (and time) on which report was updated after it had been submitted. | date |
| wasUpdatedBy | - | An entity which is updated by another entity or an agent. | xref |
| comments | - | A written explanation, observation or criticism added to textual material. | text |

### Entity: solverd_subjectinfo

Additional information about subjects included in RD3

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| subjectID&#8251; | - | An individual who is the subject of personal data, persons to whom data refers, and from whom data are collected, processed, and stored. | string |
| dateOfBirth | - | The year in which a person was born. | int |
| ageAtDeath | - | The age at which death occurred. | int |
| ageOfOnset | - | Age (in years) of onset of clinical manifestations related to the disease of the patient. | xref |
| ageAtDiagnosis | - | The age (in years), measured from some defined time point (e.g. birth) at which a patient is diagnosed with a disease. | int |
| countryOfBirth | - | The country that this person was born in. | categorical |
| consanguinity_suspected | Consanguinity suspected | subject consanguinity (yes,no) | bool |
| partOfRelease | - | One or more Solve-RD releases that indicate when the record was first introduced into RD3 or when it was updated. | mref |
| recordMetadata | - | metadata is data that provides information about data. | compound |
| dateRecordCreated | - | The date on which the activity or entity is created. | date |
| recordCreatedBy | - | Indicates the person or authoritative body who brought the item into existence. | xref |
| dateRecordUpdated | - | The date (and time) on which report was updated after it had been submitted. | date |
| wasUpdatedBy | - | An entity which is updated by another entity or an agent. | xref |
| comments | - | A written explanation, observation or criticism added to textual material. | text |

### Entity: solverd_samples

Samples used as input for analyses in RD3

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| sampleID&#8251; | - | Name or other identifier of an entry from a biosample database. | string |
| alternativeIdentifier | - | A backup sequence of characters used to identify an entity. | string |
| belongsToSubject | subjectID | An individual who is the subject of personal data, persons to whom data refers, and from whom data are collected, processed, and stored. | xref |
| ageAtSampling | ageAtSampling | age at sampling/ First contact with specialised centre (COMMON DATA ELEMENTS 4.1) | int |
| sex1 | Claimed sex | Claimed Sex / Sex at birth (can be different from observed sex, see sample) | categorical |
| sex2 | Observed sex | Computed sex (based on Y- chromosome) | categorical |
| pathologicalState | - | The pathological state of the tissue from which this material was derived. (GO:0001894) | xref |
| percentageTumorCells | - | The percentage of tumor cells compared to total cells present in this material. (NCIT:C127771) | decimal |
| tissueType | Tissue Types | - | xref |
| extractedProtocol | Extracted Protocol | - | string |
| materialType | materialType | material type. E.g. DNA | mref |
| flag | QC failed | QC failed (True /False) | bool |
| organisation | organisation | Name of the organisation. E.g. University Medical Center Groningen | xref |
| ERN | ERN | ERN | xref |
| retracted | Retracted Sample | - | categorical |
| dateAvailable | Date available | Date that a file became visible to sandbox/RD3 | date |
| anatomicalLocation | - | Biological entity that constitutes the structural organization of an individual member of a biological species from which this material was taken. | xref |
| anatomicalLocationComment | - | - | string |
| batch | - | Sample batch number | string |
| partOfRelease | - | One or more Solve-RD releases that indicate when the record was first introduced into RD3 or when it was updated. | mref |
| includedInStudies | - | Reference to the study or studies in which this person participates. | mref |
| includedInCohorts | - | A group of individuals, identified by a common characteristic. | mref |
| includedInDatasets | - | Reference to the dataset or datasets in which this person was assigned to. | mref |
| recordMetadata | - | metadata is data that provides information about data. | compound |
| dateRecordCreated | - | The date on which the activity or entity is created. | date |
| recordCreatedBy | - | Indicates the person or authoritative body who brought the item into existence. | xref |
| dateRecordUpdated | - | The date (and time) on which report was updated after it had been submitted. | date |
| wasUpdatedBy | - | An entity which is updated by another entity or an agent. | xref |
| comments | - | A written explanation, observation or criticism added to textual material. | text |

### Entity: solverd_labinfo

Information or proceesses in the lab that are linked to samples

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| experimentID&#8251; | - | - | string |
| sampleID | - | Name or other identifier of an entry from a biosample database. | mref |
| capture | Enrichment kit | Enrichment kit | string |
| libraryType | Library Source | Library Source, e.g Genomic/Transcriptomic | categorical |
| flowcell | - | flowcell information | string |
| barcode | - | A machine-readable representation of information in a visual format on a surface. | string |
| samplePosition | - | lane, or possition in well (A1 t/mH12) | string |
| library | - | link to more information about the library used in experiment | mref |
| sequencingCentre | - | An organization that provides sequence determination service | xref |
| sequencer | - | The used product name and model number of a manufacturer's genomic (dna) sequencer. | string |
| seqType | - | Method used to determine the order of bases in a nucleic acid sequence. | mref |
| arrayID | - | arrayID | string |
| mean_cov | MeanCov | Mean read depth | decimal |
| c20 | C20 | Percentage of nucleotides within the Region of interest that is covered by at least 20 reads | decimal |
| retracted | Retracted Experiment | Is the experiment retracted or not | categorical |
| partOfRelease | - | One or more Solve-RD releases that indicate when the record was first introduced into RD3 or when it was updated. | mref |
| includedInStudies | - | Reference to the study or studies in which this person participates. | mref |
| includedInCohorts | - | A group of individuals, identified by a common characteristic. | mref |
| includedInDatasets | - | Reference to the dataset or datasets in which this person was assigned to. | mref |
| recordMetadata | - | metadata is data that provides information about data. | compound |
| dateRecordCreated | - | The date on which the activity or entity is created. | date |
| recordCreatedBy | - | Indicates the person or authoritative body who brought the item into existence. | xref |
| dateRecordUpdated | - | The date (and time) on which report was updated after it had been submitted. | date |
| wasUpdatedBy | - | An entity which is updated by another entity or an agent. | xref |
| comments | - | A written explanation, observation or criticism added to textual material. | text |

### Entity: solverd_files

Individuals files stored in the sandbox environments. Files are linked to subjects, samples, and experiments, as well as with EGA accession number.

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| EGA&#8251; | - | The literal identifier for an electronic file (EGA Accession number). | string |
| name | - | The literal identifier for an electronic file. | text |
| md5 | Checksum | A 32-character hexadecimal number that is computed on a file. | string |
| fileFormat | - | type of file (BAM, FastQ, gVCF, phenopacket, BED, etc.) | categorical |
| subjectID | - | One or more subject identifier associated with a file | mref |
| sampleID | - | One more more sample identifier associated with a file | mref |
| experimentID | - | Identifier of the corresponding record in the labinfo table | xref |
| partOfRelease | - | One or more Solve-RD releases that indicate when the record was first introduced into RD3 or when it was updated. | mref |
| fenderFilePath | Fender file path | The location of the file on Fender | string |
| gearshiftFilePath | Gearshift file path | The location of the file on Gearshift | string |
| includedInStudies | - | Reference to the study or studies in which this person participates. | mref |
| includedInCohorts | - | A group of individuals, identified by a common characteristic. | mref |
| includedInDatasets | - | Reference to the dataset or datasets in which this person was assigned to. | mref |
| recordMetadata | - | metadata is data that provides information about data. | compound |
| dateRecordCreated | - | The date on which the activity or entity is created. | date |
| recordCreatedBy | - | Indicates the person or authoritative body who brought the item into existence. | xref |
| dateRecordUpdated | - | The date (and time) on which report was updated after it had been submitted. | date |
| wasUpdatedBy | - | An entity which is updated by another entity or an agent. | xref |
| comments | - | A written explanation, observation or criticism added to textual material. | text |

### Entity: solverd_overview

Overview on RD3 Subjects, samples, and experiments

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| subjectID&#8251; | - | An individual who is the subject of personal data, persons to whom data refers, and from whom data are collected, processed, and stored. | string |
| sex1 | Claimed sex | Claimed Sex / Sex at birth (can be different from observed sex, see sample) | xref |
| fid | FamilyID | A domestic group, or a number of domestic groups linked through descent (demonstrated or stipulated) from a common ancestor, marriage, or adoption. | string |
| mid | MaternalID | Identifier indicating a reference to mother of the subject | xref |
| pid | PaternalID | Identifier indicating a reference to father of the subject | xref |
| partOfRelease | - | One or more Solve-RD releases that indicate when the record was first introduced into RD3 or when it was updated. | mref |
| hasOnlyNovelOmics | - | If true, this subject does not have data in any data freeze (1,2,3,etc.) | bool |
| organisation | Organisation | Name of the organisation that submitted Subject | mref |
| ERN | ERN | ERN | mref |
| clinical_status | Affected status | whether it is an affected individual (main disease) Yes/No | bool |
| solved | - | Solved status for RD3 (True/False) | bool |
| phenotype | phenotype | Phenotype based on Human Phenotype Ontology (HPO) | mref |
| hasNotPhenotype | hasNotPhenotype | Relevant Phenotype NOT present in subject | mref |
| disease | disease | if affected, which MAIN disease code(s) apply to subject | mref |
| subjectData | - | - | compound |
| numberOfSamples | Total Number of Samples | Number of samples derived from a subject | int |
| samples | - | All samples derived from a subject | mref |
| numberOfExperiments | Total Number of Experiments | - | int |
| experiments | - | All experiments associated with a subject | mref |
| files | - | Files associated with a subject | hyperlink |

### Entity: solverd_info_cohorts

A group of individuals, identified by a common characteristic.

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| cohortID&#8251; | - | One or more characters used to identify, name, or characterize the nature, properties, or contents of a thing. | string |
| acronym | - | The non-unique initials or abbreviated name used for identification. | string |
| name | - | The words or language units by which a thing is known. | string |
| description | - | The description of the characteristics that define a cohort. | text |
| principleInvestigator | - | The principle investigator or responsible person for this study. | string |
| contactPerson | - | A person acting as a channel for communication between groups or on behalf of a group. | string |
| contactEmail | - | An email address for the purpose of contacting the study contact person. | email |
| sizeOfCohort | - | A subset of a larger population, selected for investigation to draw conclusions or make estimates about the larger population. | int |

### Entity: solverd_info_datareleases

The act of making data or other structured information accessible to the public or to the user group of a database.

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | One or more characters used to identify, name, or characterize the nature, properties, or contents of a thing. | string |
| name | - | The words or language units by which a thing is known. | string |
| description | - | A written or verbal account, representation, statement, or explanation of something. | text |
| date | - | A date of database submission refers to the moment in time in which some information was submitted/received to a database system. | date |
| createdBy | - | Indicates the person or authoritative body who brought the item into existence. | mref |
| dataSource | - | The person or authoritative body who provided the information. | mref |
| releaseNotes | - | A notation regarding the decisions, and/or clarification of any information pertaining to data management. | text |

### Entity: solverd_info_datasets

container of files belonging together (different datasets can contain overlapping files, except for the intial datasets containing startfiles (before analysis, startfiles= BAM, gVCF files (one per chromosome) and phenopacket file))

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | One or more characters used to identify, name, or characterize the nature, properties, or contents of a thing. | string |
| description | - | A written or verbal account, representation, statement, or explanation of something. | text |
| dataUseConditions | - | - | mref |
| datasetType | - | type of dataset (raw or processed (analysed) data) | enum |
| numberOfRows | - | number of records in the dataset | int |
| numberOfMales | - | a count is a data item denoted by an integer and represented the number of instances or occurences of an entity | int |
| numberOfFemales | - | a count is a data item denoted by an integer and represented the number of instances or occurences of an entity | int |
| ageCategories | - | - | mref |
| ordoCodes | - | ORDO codes en MIM codes | mref |
| hpoCodes | - | - | mref |
| comments | - | A written explanation, observation or criticism added to textual material. | text |

### Entity: solverd_info_erns

European Reference Networks, source: https://ec.europa.eu/health/ern/networks_en

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| shortname | - | - | string |
| fullname | - | - | string |
| factsheet_url | - | - | hyperlink |
| website | - | - | hyperlink |

### Entity: solverd_info_jobs

Jobs used to process sample data

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | unique identifier of the job | string |
| jobStatus | - | status of the job (running, completed, error) | enum |
| run | - | Job is part of a run | xref |
| inputFile | - | Link to the file used in the job | mref |
| outputFile | - | Link to the output file that was created at the end of the Job | mref |
| dateTimeStarted | - | Date and time the job started | datetime |
| dateTimeEnded | - | Date and time the job ended | datetime |

### Entity: solverd_info_organisations

Organisation information standardized to the Research Organisation Registry (ROR)

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| value&#8251; | - | The information contained in a data field. It may represent a numeric quantity, a textual characterization, a date or time measurement, or some other state, depending on the nature of the attribute. | string |
| description | - | A written or verbal account, representation, statement, or explanation of something | text |
| codesystem | - | A systematized collection of concepts that define corresponding codes. | string |
| code | - | A symbol or combination of symbols which is assigned to the members of a collection. | string |
| iri | - | A unique symbol that establishes identity of the resource. | hyperlink |

### Entity: solverd_info_persons

researcher or contactperson involved in the study and/or affliated with organisation

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| fullName | - | - | string |
| alternativeIdentifier | - | A backup sequence of characters used to identify an entity. | string |
| firstName | - | - | string |
| middleInitials | - | - | string |
| lastName | - | - | string |
| email | - | Email address of the contact person or organization | email |
| primaryOrganisation | - | The most significant institute for medical consultation and/or study inclusion in context of the genetic disease of this person. | xref |

### Entity: solverd_info_publications

Publication linked to subject or variants

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| doi&#8251; | - | Publication persistent identifier | hyperlink |
| title | - | The title for a published article. | string |
| journal | - | A periodical dedicated to a particular subject. | string |
| subjects | - | published paper linked to these subjects within RD3 | mref |
| variants | - | published paper linked to these variants within RD3 | mref |
| cohorts | - | published paper linked to these cohorts within RD3 | mref |
| datasets | - | published paper linked to these datasets within RD3 | mref |

### Entity: solverd_info_run

container of jobs

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| status | - | status of all jobs (in this order: failed, running, completed, so if one job is failed, status of whole run is failed) | enum |
| dataset | - | - | xref |

### Entity: solverd_info_studies

A detailed examination, analysis, or critical inspection of one or multiple subjects designed to discover facts.

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| studyID&#8251; | - | A unique proper name or character sequence that identifies this particular study. | string |
| studyAcronym | - | The non-unique initials or abbreviated name used for identification. | string |
| studyName | - | A name that designates this study. | string |
| inclusionCriteria | - | The conditions which, if met, make an person eligible for participation in this study. | text |
| principleInvestigator | - | An investigator who is responsible for all aspects of the conduct of a study. | string |
| contactPerson | - | Name of study contact. | mref |
| contactEmail | - | A text string identifier for a location to which e-mail for the study contact can be delivered. | email |
| studyDescription | - | A plan specification comprised of protocols (which may specify how and what kinds of data will be gathered) that are executed as part of this study. | text |
| studyStartDate | - | The date on which this study began. | date |
| studyCompletionDate | - | The date on which the concluding information for this study is completed. Usually, this is when the last subject has a final visit, or the main analysis has finished, or any other protocol-defined completion date. | date |
| currentStudyStatus | - | The status of a study or trial. | string |
| numberOfSubjectsEnrolled | - | An integer specifying the quantity of study subjects enrolled in the study at the current time. | int |
| samplesCollected | - | An integer specifying the quantity of samples collected at the current time. | int |
| belongsToDataRelease | - | The act of making data or other structured information accessible to the public or to the user group of a database. | mref |

### Entity: solverd_lookups_ageGroups

How long something has existed; elapsed time since birth.

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| name&#8251; | - | One or more characters used to identify, name, or characterize the nature, properties, or contents of a thing. | string |
| definition | - | A written or verbal account, representation, statement, or explanation of something | text |
| codesystem | - | A systematized collection of concepts that define corresponding codes. | string |
| code&#8251; | - | A symbol or combination of symbols which is assigned to the members of a collection. | string |
| ontologyTermURI | - | A unique symbol that establishes identity of the resource. | hyperlink |

### Entity: solverd_lookups_anatomicallocation

SNOMED CT, code list for anatomicalLocation (Snomed, Body Structure ontlogy used) See: http://bioportal.bioontology.org/ontologies/SNMD_BDY

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | Anatomical Location code | string |
| label | - | name of anatomicalLocation | string |
| ontology | - | ontology used | string |
| synonyms | - | synonyms of anatomicallocation | text |
| uri | - | link to more information | hyperlink |

### Entity: solverd_lookups_country

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| value | - | The information contained in a data field. It may represent a numeric quantity, a textual characterization, a date or time measurement, or some other state, depending on the nature of the attribute. | string |
| description | - | A written or verbal account, representation, statement, or explanation of something | text |
| codesystem | - | A systematized collection of concepts that define corresponding codes. | string |
| code&#8251; | - | A symbol or combination of symbols which is assigned to the members of a collection. | string |
| iri | - | A unique symbol that establishes identity of the resource. | hyperlink |

### Entity: solverd_lookups_dataUseConditions

code list describing different types of conditions to access the data

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | abbreviation,identifier for access conditions under which datasets are made accessible | string |
| label | - | human readable label | string |
| description | - | consentcodes according to https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4721915/ | string |

### Entity: solverd_lookups_disease

ORDO codes en MIM codes

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | Disease code | string |
| label | - | name of disease | string |
| ontology | - | ontology used | string |
| description | - | description of disease | text |
| uri | - | link to more information | hyperlink |
| parentId | - | link to parent disease | mref |
| geneLocus | - | - | string |
| geneSymbol | - | - | text |

### Entity: solverd_lookups_library

sequencing library information

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | name of library used for NGS | string |
| designDescription | - | Description of library | string |
| libraryConstructionProtocol | - | Construction Protocol library | string |
| libraryLayoutId | - | Library Layout: whether to expect SINGLE or PAIRED end reads. | string |
| pairedNominalLength | - | Nominal Length: the expected size of the insert. | string |
| librarySelectionId | - | Library Selection: the method used to select and/or enrich the material being sequenced. | string |

### Entity: solverd_lookups_libraryType

Library Source, e.g Genomic/Transcriptomic

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | name of library used for NGS | string |
| designDescription | - | Description of library | string |
| libraryConstructionProtocol | - | Construction Protocol library | string |
| libraryLayoutId | - | Library Layout: whether to expect SINGLE or PAIRED end reads. | string |
| pairedNominalLength | - | Nominal Length: the expected size of the insert. | string |
| librarySelectionId | - | Library Selection: the method used to select and/or enrich the material being sequenced. | string |
| id&#8251; | - | - | string |
| label | - | - | string |

### Entity: solverd_lookups_materialType

code list for materialType according to MIABIS-2.0-14, e.g. DNA

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| label | - | name of disease | string |
| source | - | source used | string |
| description | - | description of disease | text |
| uri | - | link to more information | hyperlink |

### Entity: solverd_lookups_noyesunknown

Categories No, Unknown, Yes

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| label | - | - | string |
| description | - | - | string |
| codesystem | - | - | string |
| code | - | - | string |
| iri | - | - | hyperlink |

### Entity: solverd_lookups_pathologicalstate

None

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| value&#8251; | - | The information contained in a data field. It may represent a numeric quantity, a textual characterization, a date or time measurement, or some other state, depending on the nature of the attribute. | string |
| description | - | A written or verbal account, representation, statement, or explanation of something | text |
| codesystem | - | A systematized collection of concepts that define corresponding codes. | string |
| code | - | A symbol or combination of symbols which is assigned to the members of a collection. | string |
| iri | - | A unique symbol that establishes identity of the resource. | hyperlink |

### Entity: solverd_lookups_phenotype

HPO code list for phenotype, version 2020-02-27 http://bioportal.bioontology.org/ontologies/HP

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | HPO Code | string |
| label | - | label of the phenotype | string |
| description | - | description of the phenotype | text |
| synonyms | - | synonyms of the phenotype | text |
| uri | - | link to more information | hyperlink |
| parents | - | link to parent phenotype(s) | mref |

### Entity: solverd_lookups_seqType

Sequencing technique types (in CNAG batchfile = ExpType)

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| label | - | - | string |

### Entity: solverd_lookups_sex

code list for sex. According to MIABIS-2.0-09, E.g. 'M'

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| label | - | - | string |
| description | - | - | string |

### Entity: solverd_lookups_tissueType

TissueTypes, source is GTeX;  https://www.gtexportal.org/home/tissueSummaryPage

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| label | - | - | string |
| description | - | - | string |
| codesystem | - | - | string |
| code | - | - | string |
| iri | - | - | hyperlink |

### Entity: solverd_lookups_typeFile

type of x (e.g. BAM, gVCF, phenopacket, BED, etc.)

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| label | - | - | string |
| description | - | - | string |
| codesystem | - | - | string |
| code | - | - | string |
| iri | - | - | hyperlink |

### Entity: solverd_variants_assembly

Human reference sequence used (https://genome-euro.ucsc.edu/cgi-bin/hgGateway?redirect=manual&source=genome.ucsc.edu)

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| label | - | - | string |

### Entity: solverd_variants_classification

clinical classification (https://blueprintgenetics.com/variant-classification/)

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| id&#8251; | - | - | string |
| label | - | - | string |

### Entity: solverd_variants_variant

variant information according toHGVS (http://varnomen.hgvs.org/)

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| hgvs_cdna&#8251; | - | hgvs notation for this allele. Format (NM_004006.2:c.4375C>T) | string |
| genomeBuild | - | Human Assembly, genomic build. Format (Mar.2006 (NCBI36/hg18)) | categorical |
| chromosome | - | chromosome of variant | int |
| start_gPosition | - | genomic start position | int |
| stop_gPosition | - | genomic stop position | int |
| ref | - | Reference allele | string |
| alt | - | Alternative allele | string |
| variantType | - | variantType | categorical |

### Entity: solverd_variants_variantType

Sequence variant types (https://varnomen.hgvs.org/recommendations/DNA)

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| hgvs_cdna&#8251; | - | hgvs notation for this allele. Format (NM_004006.2:c.4375C>T) | string |
| genomeBuild | - | Human Assembly, genomic build. Format (Mar.2006 (NCBI36/hg18)) | categorical |
| chromosome | - | chromosome of variant | int |
| start_gPosition | - | genomic start position | int |
| stop_gPosition | - | genomic stop position | int |
| ref | - | Reference allele | string |
| alt | - | Alternative allele | string |
| variantType | - | variantType | categorical |
| id&#8251; | - | - | string |
| label | - | - | string |

### Entity: solverd_variants_variantinfo

variantinfo specific per subject

| Name | Label | Description | Data Type |
|:---- |:-----|:-----------|:---------|
| hgvs_cdna&#8251; | - | hgvs notation for this allele. Format (NM_004006.2:c.4375C>T) | string |
| genomeBuild | - | Human Assembly, genomic build. Format (Mar.2006 (NCBI36/hg18)) | categorical |
| chromosome | - | chromosome of variant | int |
| start_gPosition | - | genomic start position | int |
| stop_gPosition | - | genomic stop position | int |
| ref | - | Reference allele | string |
| alt | - | Alternative allele | string |
| variantType | - | variantType | categorical |
| variantID&#8251; | - | - | string |
| variant | - | link to variant | xref |
| classification | - | clinical classification (https://blueprintgenetics.com/variant-classification/) | categorical |
| extra | - | extra variantInfo | compound |
| gene | - | gene of variant | string |
| exon | - | exon(s) involved in variant.Format (NM_004006.2:exon3) | string |
| protein | - | p. notation. Format(NP_003997.1:p.Lys23_Val25del) | string |

Note: The symbol &#8251; denotes attributes that are primary keys

