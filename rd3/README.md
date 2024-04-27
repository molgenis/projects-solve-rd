# RD3 Scripts

In this document, you can find notes about each script that is located in this folder.

## solverd_tree_mapping.py: Preparing the tree dataset

Since most of the data is available in the "Solve-RD Experiments" table, we
can use it as the source for the patient tree dataset. For the UI, we would like
the data to be searchable for subject- and family identifiers rather than
everything. It is also better to do as much preprocessing to eliminate the
need to transform the data in javascript. The structure of the data should
look like the following.

| subjectID      | familyID      | json       |
|----------------|---------------|------------|
| `<patient-id>` | `<family-id>` | `"{...}"`  |

The value in the json column should contain everything that is needed in
Vue component. The structure of the json object should look like this.

```text
 - Patient
   - Sample n
     - Experiment n
     ....
   ....
```

```json
{
  "id": "<some-identifier>",
  "subjectID": "<rd3-subject-identifier>",
  "familyID": "<rd3-family-identifier>",
  "group": "patient",
  "href": "<url-to-subject-record>",
  "children": [
    {
      "id": "<some-identifier>+n",
      "name": "sample-idenfier",
      "group": "sample",
      "href": "<url-to-sample-record>",
      "children": [
        {
          "id": "<some-identifier>+n",
          "name": "<experiment-idenfier>",
          "group": "experiment",
          "href": "<url-to-experiment-record>",
        },
        ....
      ]
    },
    ....
  ]
}
```

Entries should be rendered for all subjects regardless if they have sample
or experiment metadata (as this is important to know). Build a list of
subjects from the Subjects table, and then add samples and experiments.
