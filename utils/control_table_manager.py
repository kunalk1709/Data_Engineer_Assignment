# Databricks notebook source
# DBTITLE 1, Import Modules
import traceback
from pyspark.sql.functions import current_timestamp, date_format
# COMMAND ----------

# DBTITLE 1, Control Class with Funcs
class ControlTableManager:
    # Manages control table operations for pipeline orchestration
    def __init__(self, spark, Control_Table):
        self.spark = spark
        self.control_table = Control_Table

    # Fetch control info
    def get_control_info(self, target_table, source_path_table):
        # Retrieves control information for a specific table
        try:
            sql_script = f"""
                SELECT * FROM {self.control_table} WHERE target_table = '{target_table}' AND source_path_table = '{source_path_table}' AND is_active = 'Y'
            """
            result = self.spark.sql(sql_script)
            if result:
                return result.limit(1)
            else:
                print(f"-> No active control record found of target table: {target_table} having source table {source_path_table}")
                return None
        except Exception as e:
            print(f"Error getting control info: {str(e)}")
            print(traceback.format_exc())
            raise

    # Update control table
    def update_control_table(self,source_path_table,target_table,last_run_status,records_processed,new_watermark=None):
        # Updates control table with job execution results
        try:
            if new_watermark is None:
                sql_script = f"""
                    UPDATE {self.control_table}
                    SET last_run_timestamp = current_timestamp(),last_run_status = '{last_run_status}',records_processed = {records_processed},updated_timestamp = current_timestamp()
                    WHERE target_table = '{target_table}' AND source_path_table = '{source_path_table}'
                """
            else:
                sql_script = f"""
                    UPDATE {self.control_table}
                    SET last_run_timestamp = current_timestamp(),last_run_status = '{last_run_status}',records_processed = {records_processed},last_watermark = '{new_watermark}',updated_timestamp = current_timestamp()
                    WHERE target_table = '{target_table}' AND source_path_table = '{source_path_table}'
                """
            self.spark.sql(sql_script)
            print(f"-> Control table updated for {target_table}")
            return True
        except Exception as e:
            print(f"Error updating control table: {str(e)}")
            print(traceback.format_exc())
            raise

    # Check if table is active
    def is_table_active(self, target_table, source_path_table):
        # Checks if a table is active for processing
        try:
            sql_script = f"""
                SELECT is_active FROM {self.control_table} WHERE target_table = '{target_table}' AND source_path_table = '{source_path_table}'
            """
            result = self.spark.sql(sql_script).collect()
            return result and result[0][0] == 'Y'
        except Exception as e:
            print(f"Error checking table active status: {str(e)}")
            print(traceback.format_exc())
            raise

    # Get last watermark
    def get_last_watermark(self, target_table, source_path_table):
        # Gets the last watermark timestamp for incremental processing
        try:
            sql_script = f"""
                SELECT last_watermark FROM {self.control_table} WHERE target_table = '{target_table}' AND is_active = 'Y' AND source_path_table = '{source_path_table}'
            """
            result = self.spark.sql(sql_script).collect()
            if result and result[0][0]:
                return result[0][0]
            else:
                return None
        except Exception as e:
            print(f"Error fetching the last watermark value for table {target_table}: {str(e)}")
            print(traceback.format_exc())
            raise

print("Control Table Manager Utility Imported")

# COMMAND ----------

# DBTITLE 1, Test Functions
# import os
# import datetime
# from pyspark.sql.functions import col

# control_manager = ControlTableManager(spark,"cat_dfc_dev_system.dfc_system.tbl_sys_control")
# if not control_manager.is_table_active("otp_eo_txn.tbl_txn_eo_acdoca","edh_unreg_gold_run.essent_s4u.acdoca"):
#     raise Exception("Table 'otp_eo_txn.tbl_txn_eo_acdoca' is not active in control table")

# control_info = control_manager.get_control_info("otp_eo_txn.tbl_txn_eo_acdoca","edh_unreg_gold_run.essent_s4u.acdoca")
# control_info.display()

# control_manager.update_control_table(source_path_table="edh_unreg_gold_run.essent_s4u.acdoca",target_table="otp_eo_txn.tbl_txn_eo_acdoca",last_run_status="SUCCESS",records_processed=0,new_watermark="2025-12-11 18:15:42")

# %sql
# SELECT *, date_format(last_run_timestamp, 'yyyy-MM-dd HH:mm:ss') FROM cat_dfc_dev_system.dfc_system.tbl_sys_control
