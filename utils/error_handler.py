# Databricks notebook source
# DBTITLE 1, Import Modules

import os
import traceback
from pyspark.sql.functions import current_timestamp, date_format

# COMMAND ----------

# DBTITLE 1, ErrorHandler Class with Func start_audit & end_audit
class ErrorHandler:
    """
    Handles error logging and exception management
    """
    def __init__(self, spark, Error_Log_Table):
        self.spark = spark
        self.error_log_table = Error_Log_Table
    def log_error(self,job_id,job_run_id,notebook_name,error_type,error_message,error_stack_trace,table_name,severity,created_by):
        """
        Logs error information to error log table
        """
        try:
            # Escape single quotes and trim long text
            def sanitize(value, max_len=4000):
                if value is None:
                    return ''
                return value.replace("'", "''")[:max_len]

            error_message = sanitize(error_message)
            error_stack_trace = sanitize(error_stack_trace)

            sql_script = f"""
                INSERT INTO {self.error_log_table} (job_id,job_run_id,notebook_name,error_type,error_message,error_stack_trace,table_name,severity,created_timestamp,created_by)
                VALUES ('{job_id}','{job_run_id}','{notebook_name}','{error_type}','{error_message}','{error_stack_trace}','{table_name}','{severity}',current_timestamp(),'{created_by}')
            """
            self.spark.sql(sql_script)
            print(f"Error logged - Job ID: {job_id}, Type: {error_type}")
            return True
        except Exception as e:
            print(f"Critical error logging failure: {str(e)}")
            print(traceback.format_exc())
            raise

print("Error Log Utility Imported")

# COMMAND ----------

# DBTITLE 1, Test Function
# import os
# import traceback

# error_logger = ErrorHandler(spark,'cat_dfc_dev_system.dfc_system.tbl_sys_error')

# # Get job_id and run_id from environment variables
# job_id = os.getenv("DB_JOB_ID")
# run_id = os.getenv("DB_JOB_RUN_ID")
# # Get Databricks notebook name along with path
# context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
# notebook_path_name = context.notebookPath().getOrElse(None)
# # Get the executed_by either job or user
# executed_by = os.getenv("DB_USER")
# if not executed_by:
#     context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
#     executed_by = str(context.userName()).replace("Some(", "").replace(")", "")

# # Below code to fail intentionally so that we can test the error logging
# try:
#     sql_script = """
#         SELECT * FROM non_existing_table
#     """
#     spark.sql(sql_script)
#     print("Success")

# except Exception as e:
#     error_logger.log_error(job_id=job_id,job_run_id=run_id,notebook_name=notebook_path_name,error_type=type(e).__name__,error_message=str(e),error_stack_trace=str(traceback.format_exc()),table_name="tbl_Testing",severity="Error",created_by=executed_by)
#     print(f"Critical error logging failure: {str(e)}")
#     print(traceback.format_exc())
#     raise

# COMMAND ----------
# %sql
# SELECT *, date_format(created_timestamp, 'yyyy-MM-dd HH:mm:ss') FROM cat_dfc_dev_system.dfc_system.tbl_sys_error

