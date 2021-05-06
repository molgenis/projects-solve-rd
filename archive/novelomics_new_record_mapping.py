# @title Map new RD3 records
# @description for new records, map them to the intermediate table
# @param data input dataset to process
# @param id_suffix content to append to ID, e.g., "_original"
# @return a list of dictionaries
def map_rd3_new_records(data, id_suffix):
    out = []
    for d in data:
        tmp = {}
        tmp['id'] = d.get('subject_id') + id_suffix
        tmp['EGA'] = d.get('file_ega_id')
        tmp['EGApath'] = d.get('file_path')
        tmp['name'] = d.get('file_name')
        tmp['md5'] = d.get('unencrypted_md5_checksum')
        tmp['typeFile'] = d.get('file_type')
        tmp['filegroupID'] = d.get('file_group_id')
        tmp['batchID'] = d.get('batch_id')
        tmp['experimentID'] = d.get('project_experiment_dataset_id')
        tmp['sampleID'] = d.get('sample_id')
        tmp['subjectID'] = d.get('subject_id')
        tmp['sequencingCentre'] = d.get('sequencing_center')
        tmp['sequencer'] = d.get('platform_model')
        tmp['libraryType'] = d.get('library_source').title()
        tmp['capture'] = d.get('library_selection')
        tmp['seqType'] = d.get('library_strategy')
        tmp['libraryLayout'] = d.get('library_layout')
        tmp['tissueType'] = d.get('tissue_type')
        tmp['materialType'] = d.get('sample_type')
        tmp['project_batch_id'] = d.get('project_batch_id')
        tmp['run_ega_id'] = d.get('run_ega_id')
        tmp['experiment_ega_id'] = d.get('experiment_ega_id')
        out.append(tmp)
    return out