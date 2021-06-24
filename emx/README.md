# RD3 EMX Files

In this folder, you can find all of the EMX (v1) files used in RD3. These files are described in the following table.

| folder              | molgenisID              |
|---------------------|-------------------------|
| `freeze`            | `rd3_freeze[X]`         |
| `novelomics`        | `rd3_novelomics`        |
| `portal-freeze`     | `rd3_portal_freeze`     |
| `portal-novelomics` | `rd3_portal_novelomics` |

## Freeze

The file `rd3_freezeX.xlsx` is a template for new RD3 releases. Before importing into the database, make sure all instances of `freezeX` are replaced with the current release (e.g., `freeze3`, `freeze10`, etc.) and make sure the filename reflects the new freeze. For example, if you are creating a new subpackage for `freeze50`, rename the filename to `rd3_freeze50.xlsx`.

```python
mcmd config set host
mcmd import -p emx/freeze/rd3_freezeX.xlsx # replace X with current release number
```

The following entities will be created.

- `rd3_freezeX`: the new release
    - `rd3_novelomics_file`
    - `rd3_novelomics_labinfo`
    - `rd3_novelomics_sample`
    - `rd3_novelomics_subject`
    - `rd3_novelomics_subjectinfo`

## Novel Omics

The `rd3_novelomics.xslx` file contains package, entities, and attributes for the novel omics release in RD3. These tables mirror the freeze structure, but the lab information tables differ as there are different experiments in this release. This file will create the novelomics subpackage within the main `rd3` package.

```python
mcmd config set host
mcmd import -p emx/novelomics/rd3_novelomics.xlsx
```

The entity IDs are listed below.

- `rd3_novelomics`: subpackage ID
    - `rd3_novelomics_file`: files table
    - `rd3_novelomics_labinfo_wgs`: experiment information for WGS
    - `rd3_novelomics_sample`: sample metadata
    - `rd3_novelomics_subject`: patient metadata
    - `rd3_novelomics_subjectinfo`: addition patient metadata

## Portal Freeze

The `rd3_portal_freeze.xlsx` file is used to create an intermediate table in RD3 which can be used for storing raw freeze metadata. This creates a new table within the portal that is processed and pushed into the corresponding freeze.

```python
mcmd config set host
mcmd import -p emx/portal-freeze/rd3_portal_freeze.xlsx
```

This table can be called in scripts using the following entityID: `rd3_portal_freeze`.

## Portal Novel Omics

Importing `rd3_portal_novelomics.xlsx` into RD3 will create a subpackage in the RD3 portal and two tables. These tables are used as a staging for raw novel omics metadata which are regularly processed and pushed into the Novel Omics tables (created by `rd3_novelomics.xlsx`).

```python
mcmd config set host
mcmd import -p emx/portal-novelomics/rd3_portal_novelomics.xlsx
```

The entity IDs created by this file are listed below.

- `rd3_portal_novelomics`: Novel omics subpackage within the RD3 Portal
    - `rd3_portal_novelomics_experiment`: raw novel omics experiment metadata
    - `rd3_portal_novelomics_shipment`: raw samples and patient metadata