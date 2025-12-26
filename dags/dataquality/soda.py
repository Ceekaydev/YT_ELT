import logging
from airflow.operators.bash import BashOperator
import os

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"

CHECKS_BY_SCHEMA = {
    "staging": f"{SODA_PATH}/staging_checks.yml",
    "core": f"{SODA_PATH}/core_checks.yml",
}

def yt_elt_data_quality(schema: str):

    if schema not in CHECKS_BY_SCHEMA:
        raise ValueError(
            f"Unsupported schema '{schema}'."
            f"Expected one of {list(CHECKS_BY_SCHEMA.keys())}"
        )
    
    checks_file = CHECKS_BY_SCHEMA[schema]

    try:

        # Prepare environment variables for Soda
        env_vars = os.environ.copy()
        env_vars['SCHEMA'] = schema

        task = BashOperator(
            task_id = f'soda_test_{schema}',
            bash_command=(
                f"soda scan "
                f"-d {DATASOURCE} "
                f"-c {SODA_PATH}/configuration.yml "
                f"-v SCHEMA={schema} "
                f"{checks_file}"
            ),
            env=env_vars,  # ✅ Add this line - passes all environment variablesdz
        )

        return task
    
    except Exception as e:
        logger.error(f"Error creating Soda data quality task for schema {schema}: {e}")
        raise e