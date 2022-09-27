# Changelog for rd3.yaml

## Version 2.0-beta-0.9.0

- Primary RD3 tables were moved up an level and reference tables were moved to a nested folder. Referenced tables IDs are now formatted like so: `rd3_lookups_<table>`
- Pluralized all primary tables: `subjects`, `samples`, and `files`. The table `subjectinfo` and `labinfo` remains the same.
- All ref entities were changed to `rd3_lookups-*`
- Added ontology tags where possible; preparation for Beacon integration
- renamed `patch` to `partOfRelease`
- removed `patch_comment` as this should go in the `releases` lookup
- added compound attribute for row-level metadata to all tables, i.e., `recordMetadata` that has
the columns `comments`, `dateRecordCreated`, `recordCreatedBy`, `dateRecordUpdated`, `wasUpdatedBy`
- ERNs are now stored in the `organisations` reference table. Please use `rd3_lookups_organisations`

- subjects table updates: removed `id`; `subjectID` is now the primary key
- subjects table updates: renamed `phenopacketID` to `mostRecentPhenopacketFile`

- subjectinfo updates: removed `id`; `subjectID` is now the primary key
- subjectinfo updates: renamed `dateofBirth` to `dateOfBirth`
- subjectinfo updates: renamed `ageOfDeath` to `ageAtDeath`
- subjectinfo updates: renamed `ageOfDiagnosis` to `ageAtDiagnosis`
- subjectinfo updates: renamed `Country_of_origin` to `countryOfBirth`

- samples updates: removed `id`; `sampleID` is now the primary key
- samples updates: renamed `anatomicalLocation` to `anatomicalSource`

- labinfo updates: removed `id`; `experimentID` is now the primary key
- labinfo updates: changed data type of `sequencingCenter` from `string` to `xref`

- files updates: renamed `typeFile` to `fileFormat`
- files updates: added `subjectID`: reference `rd3_subjects`
- files updates: added `sampleID`: references `rd3_samples`
- files updates: added `experimentID`: references `rd3_labinfo`
- files updates: renamed `filepath_sandbox` to `fenderFilePath`
- files updates: added `gearshiftFilePath`
- files updates: renamed `extraInfo` to `comments` and moved to `recordMetadata`
  