# Changelog for rd3.yaml

## Version 2.0-beta-0.9.0

- Pluralized all primary entities: `subjects`, `samples`, etc.
- Updated the `files` table
    - renamed table identifier from `EGA` to `fileID`
    - renamed `typeFile` to `fileFormat`
    - added `belongsToSubject`: reference `rd3_subjects`
    - added `belongsToSample`: references `rd3_samples`
    - added `belongsToExperiment`: references `rd3_labinfo`
    - renamed `patch` to `belongsToRelease`
    - renamed `filepath_sandbox` to `fenderFilePath`
    - added `gearshiftFilePath`
    - removed `patch_comment`: this is stored in the new releases table
    - added compound attribute for row-level metadata, i.e., `recordMetadata`
    - renamed `dateCreated` to `dateRecordCreated`
    - added `recordCreatedBy`, `dateRecordUpdated`, `wasUpdatedBy` to compound `recordMetadata`
    - renamed `extraInfo` to `comments` and moved to `recordMetadata`
  