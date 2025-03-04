from ods_data_processing.batch.batch_types import BatchConfig, FileFormat, \
    TransformParams, LogParams, ValidationParams, CSVDataSource, DataOutput, \
    FeatureParams, EnabledFlag, DataSource, PheonixDataSource, SourceType
from ods_data_processing.batch.error import PipelineConfigurationError
from ods_data_processing.utils import s3_source_path, s3_alt_path, s3_log_path, \
    full_table_name

def pipeline_task_id(
        pipeline_name: str,
        pipeline_task_name: str
) -> str:
    return f'{pipeline_name}_{pipeline_task_name}'

def create_data_source(
        generic_args: dict,
        job_args: dict,
        dag_args: dict
) -> DataSource:
    
    source_type = SourceType[job_args['source_type'].upper()]

    if source_type == SourceType.FILE:

        source_data_path = s3_source_path(
            generic_args,
            job_args,
            dag_args,
            dag_args['src_file']
        )

        source_ctl_path = s3_source_path(
            generic_args,
            job_args,
            dag_args,
            dag_args['ctl_file']
        )

        multiline_rows = job_args.get('source_data_csv', {}).get('multiline_rows', False)
        if not isinstance(multiline_rows, bool):
            raise PipelineConfigurationError("Invalid CSV data source multiline value")
        
        return CSVDataSource(
            source_type = source_type,
            format = FileFormat[job_args.get('source_data_file_format', 'CSV').upper()],
            path = source_data_path,
            delimiter = job_args['source_data_delimiter'],
            ctl_path = source_ctl_path,
            processed_path = s3_alt_path(source_data_path, 'processed'),
            rejected_path = s3_alt_path(source_data_path, 'rejected_files'),
            processed_ctl_path = s3_alt_path(source_ctl_path, 'processed'),
            rejected_ctl_path = s3_log_path(source_ctl_path, 'rejected_files'),
            rejected_records_path = s3_alt_path(source_data_path, 'rejected_records'),
            file_mode = job_args.get('spark_read_file_mode', 'FAILFAST'),
            escape_chr = job_args.get('source_data_escape_character', '"'),
            multiline_rows = multiline_rows
        )
    
    elif source_type == SourceType.DATABASE:
        return PheonixDataSource(
            source_type = source_type,
            table = full_table_name(job_args.get('source_db_name'),
                                    job_args.get('source_table_name')),
            zkurl = generic_args['zkurl'],
            connector = generic_args['connector']
        )

def create_batch_config(generic_args, job_args, dag_args) -> BatchConfig:

    feature_enabled = FeatureParams(
        data_processing=EnabledFlag[
            job_args.get('enable_data_processing', 'Y').upper()
        ].value,
        input=EnabledFlag[
            job_args.get('enable_input', 'Y').upper()
        ].value,
        file_validation=EnabledFlag[
            job_args.get('enabled_file_validation', 'Y').upper()
        ].value,
        output=EnabledFlag[
            job_args.get('enable_output', 'Y').upper()
        ].value,
        post_processing = EnabledFlag[
            job_args.get('enable_post_processing', 'Y').upper()
        ].value
    )

    data_source = create_data_source(generic_args, job_args, dag_args)

    log_params = LogParams(
        log_level=job_args.get('log_level', 'INFO'),
        log_path=s3_log_path(dag_args, generic_args),
        run_id=dag_args['job_id']
    )

    validation_params = ValidationParams(
        config_path=generic_args['record_validation_conf_path'],
        rejected_records_limit=job_args.get('record_validation_limit', 0)
    )

    transform_params = TransformParams(
        reference_data=generic_args['transformation_reference_data_path'],
        source_data=generic_args['transformation_source_data_path'],
        mapping=generic_args['transformation_mapping_path'],
        reference_schema_mapping=job_args['reference_schema_mapping'],
    )

    data_output = DataOutput(
        table = full_table_name(job_args['target_db_name'],
                                job_args['output_table_name']),
        zkurl=generic_args['zkurl'],
        connector=generic_args['connector'],
    )

    return BatchConfig(
        feature_enabled=feature_enabled,
        batch_process_name=generic_args['batch_process_name'],
        pipeline_name=dag_args['pipeline_name'],
        pipeline_task_id=pipeline_task_id(
            dag_args['pipeline_name'], dag_args['pipeline_task_name']
        ),
        task_id=dag_args['task_id'],
        log_params=log_params,
        validation_params=validation_params,
        transform_params=transform_params,
        data_source=data_source,
        data_output=data_output,
    )

