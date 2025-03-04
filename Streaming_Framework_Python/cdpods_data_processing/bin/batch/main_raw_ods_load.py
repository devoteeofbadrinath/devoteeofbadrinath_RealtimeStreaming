import dataclasses as dtc
import sys
import os
import logging
from ods_data_processing.batch.app import STAGE_DISABLED, application_stage, application_context

current_dir = os.path.dirname((os.path.abspath(__file__)))
parent_dir = os.path.abspath(os.path.join(current_dir, '../../'))
sys.path.insert(0, parent_dir)

from ods_data_processing.batch import input_module
from ods_data_processing.batch import transform_data
from ods_data_processing.batch import output_module
from ods_data_processing.batch.batch_types import ValidationType, AppStage
from ods_data_processing.batch import validation_module
from ods_data_processing.batch import record_validation_module

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    with application_context() as ctx, application_stage(AppStage.DATA_PROCESSING):
        with application_stage(AppStage.INPUT):
            ppl_data = input_module.read_data_source(ctx)

        if ctx.config.feature_enabled[AppStage.FILE_VALIDATION.value]:
            with application_stage(AppStage.FILE_VALIDATION):
                validation_module.validate_file(ctx, ppl_data)

        else:
            logger.info(STAGE_DISABLED, AppStage.FILE_VALIDATION.value)

        with application_stage(AppStage.PRE_RECORD_VALIDATION):
            ppl_data = record_validation_module.validate_records(
                ctx, ppl_data, ValidationType.PRE_TRANSFORM
            )

        with application_stage(AppStage.TRANSFORMATION):
            transformed_df = transform_data(ctx, ppl_data.data)

        with application_stage(AppStage.POST_RECORD_VALIDATION):
            ppl_data = record_validation_module.validate_records(
                ctx,
                dtc.replace(ppl_data, data = transformed_df),
                ValidationType.POST_TRANSFORM
            )

        with application_stage(AppStage.OUTPUT):
            output_module.put_records(ppl_data.data, ctx.config)
