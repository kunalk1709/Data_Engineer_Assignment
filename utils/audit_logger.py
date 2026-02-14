# Databricks notebook source
# DBTITLE 1, Import Modules
import traceback
from pyspark.sql.functions import current_timestamp, date_format

# COMMAND ----------

# DBTITLE 1, Audit Class with Func start_audit & end_audit
class AuditLogger:
    # Handles audit logging for pipeline operations
    def __init__(self, spark, Audit_Table):
        self.spark = spark
        self.audit_table = Audit_Table

    # Start audit
    def start_audit(self,job_id,job_run_id,notebook_name,source_layer,source_path_table,target_layer,target_table,operation_type,created_by):
        # Logs the start of a job execution, Returns audit_id for tracking.
        try:
            sql_script = f"""
                INSERT INTO {self.audit_table} (job_id,job_run_id,notebook_name,source_layer,source_path_table,target_layer,target_table,operation_type,records_read,records_inserted,records_updated,records_deleted,status,start_time,end_time,error_message,created_by)
                VALUES ('{job_id}','{job_run_id}','{notebook_name}','{source_layer}','{source_path_table}','{target_layer}','{target_table}','{operation_type}',NULL,NULL,NULL,NULL,'RUNNING',current_timestamp(),NULL,NULL,'{created_by}')
            """
            self.spark.sql(sql_script)
          
            # Fetch audit_id of inserted record
            sql_script = f"""
                SELECT audit_id FROM {self.audit_table} WHERE job_id = '{job_id}' AND start_time = ( SELECT MAX(start_time) FROM {self.audit_table} WHERE job_id = '{job_id}' ) AND job_run_id = '{job_run_id}' AND notebook_name = '{notebook_name}'
            """
            audit_id = self.spark.sql(sql_script).collect()[0][0]
            print(f"-> Audit started - Audit ID: {audit_id}")
            return audit_id
        except Exception as e:
            print(f"Error starting audit: {str(e)}")
            print(traceback.format_exc())
            raise

    # End audit
    def end_audit(self,audit_id,status,records_read,records_inserted,records_updated,records_deleted,error_message):
        # Updates audit log with completion information
        try:
            # Escape single quotes and trim long text
            def sanitize(value, max_len=4000):
                if value is None:
                    return ""
                return value.replace("'", "''")[:max_len]

            error_message = sanitize(error_message)

            sql_script = f"""
                UPDATE {self.audit_table}
                SET
                    records_read = {records_read if records_read else 'NULL'},
                    records_inserted = {records_inserted if records_inserted else 'NULL'},
                    records_updated = {records_updated if records_updated else 'NULL'},
                    records_deleted = {records_deleted if records_deleted else 'NULL'},
                    status = '{status}',
                    end_time = current_timestamp(),
                    error_message = {'NULL' if not error_message else f"'{error_message}'"}
                WHERE audit_id = {audit_id}
            """
            self.spark.sql(sql_script)
            print(f"-> Audit completed - Audit ID: {audit_id}, Status: {status}")
            return True
        except Exception as e:
            print(f"Error ending audit: {str(e)}")
            print(traceback.format_exc())
            raise

print("Audit Logger Utility Imported")

# COMMAND ----------

# DBTITLE 1, Test Function
# import os
# import datetime

# audit_logger = AuditLogger(spark,"cat_dfc_dev_system.dfc_system.tbl_sys_audit")

# # Get job id and run id from environment variables
# job_id = os.getenv("DB_JOB_ID")
# run_id = os.getenv("DB_JOB_RUN_ID")

# # Get Databricks notebook name along with path
# context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
# notebook_path_name = context.notebookPath().getOrElse(None)
# # Get the executed_by (either job or user)
# executed_by = os.getenv("DB_USER")
# if not executed_by:
#     executed_by = str(context.userName()).replace("Some(", "").replace(")", "")

# audit_id = audit_logger.start_audit(job_id=job_id,job_run_id=run_id,notebook_name=notebook_path_name,source_layer="Landing",source_path_table="data/ACDOCA_E0/",target_layer="Output",target_table="testing",operation_type="Merge",created_by=executed_by)
# audit_logger.end_audit(audit_id=audit_id,status="SUCCESS",records_read=1000,records_inserted=1000,records_updated=0,records_deleted=0,error_message=None)

# %sql
# SELECT *, date_format(start_time, 'yyyy-MM-dd HH:mm:ss') FROM cat_dfc_dev_system.dfc_system.tbl_sys_audit
