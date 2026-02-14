# Databricks notebook source
# DBTITLE 1, Import Util Files
# MAGIC %run ../utils/Environment_Variables_Config

# COMMAND ----------
# DBTITLE 1, Import Modules
import os
import traceback
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (current_timestamp, date_format, col, lit, sha2,concat_ws, coalesce, when, to_timestamp,to_date, current_date)
from pyspark.sql.types import (StructType, StructField, IntegerType,StringType, TimestampType, ArrayType, MapType)
from typing import Literal
from typing_extensions import Literal
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

# COMMAND ----------
# DBTITLE 1, Variable_Set / Initialize_Utilities / Job_Metadata
Catalog_System = catalog_system
Catalog_Data = catalog_data

Table_Audit = f"{Catalog_System}.dfc_system_poc.tbl_sys_audit"
Table_Control = f"{Catalog_System}.dfc_system_poc.tbl_sys_control"
Table_Error = f"{Catalog_System}.dfc_system_poc.tbl_sys_error"
print(f"Catalogs: {Catalog_Data}, {Catalog_System}")
print(f"System_Tables: {Table_Audit}, {Table_Control}, {Table_Error}")

Audit_Logger = AuditLogger(spark, Table_Audit)
Control_Manager = ControlTableManager(spark, Table_Control)
Data_Validation = DataValidation(spark)
Error_Logger = ErrorHandler(spark, Table_Error)
Data_Process = DataProcess(spark)

# Job metadata
Job_Id = os.getenv("DB_JOB_ID")
Run_Id = os.getenv("DB_JOB_RUN_ID")
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
Notebook_Path_Name = context.notebookPath().getOrElse(None)

Executed_By = os.getenv("DB_USER")
if not Executed_By:
    Executed_By = str(context.userName()).replace("Some(", "").replace(")", "")
print(f"Job_Id: {Job_Id}, Job_Run_Id: {Run_Id}, Notebook_Path_Name: {Notebook_Path_Name}, Executed_By: {Executed_By}")

# COMMAND ----------
# Load Dimemsion Tables (Customer and Product) having Full Load
try:
    Audit_Id = None
    Source_Table = None
    # Get target tables for Output JO layer
    df_target_tables = (spark.table(Table_Control).filter((col("target_layer") == "Silver") &(col("target_table_type") == "Dim") &(col("Load_Type") == "FullLoad") &(col("target_table").contains("dim_data")))
        .select(col("target_table")).dropna().distinct())
    df_target_tables.display(truncate=False)
    for tgt in df_target_tables.collect():
        Target_Table = tgt["target_table"]
        Target_Table_with_cat = f"{Catalog_Data}.{Target_Table}"
        print(f"Target_Table: {Target_Table} starts")
        Target_Table_Fields = spark.table(Target_Table_with_cat).columns
        # Check if already loaded today
        df_check_target_loaded_today = spark.table(Table_Control).filter(
            (F.col("target_table") == Target_Table) &
            ( (F.to_date(F.col("last_run_timestamp")) != current_date()) | (F.col("last_run_status") != "SUCCESS") ) )
        if df_check_target_loaded_today.count() == 0:
            print(f"-> Target table {Target_Table} is already loaded for today. Skipping.")
            continue
        tgt_delta = DeltaTable.forName(spark, Target_Table_with_cat)
        tgt_delta.delete("1=1")
        print(f"-> Target table {Target_Table} is truncated.")
        df_source_tables = (spark.table(Table_Control).filter(col("target_table") == Target_Table).select(col("source_path_table").alias("source_table")).dropna().distinct())

        for src in df_source_tables.collect():
            Source_Table = src["source_table"]
            print(f"source_table: {Source_Table} and target_table: {Target_Table_with_cat}")

            if not Control_Manager.is_table_active(target_table=Target_Table,source_path_table=Source_Table):
                raise Exception(f"Table {Target_Table} for source {Source_Table} is Inactive in control table")
            print(f"-> target table {Target_Table} for source {Source_Table} is active in control table {Table_Control}")

            Audit_Id = Audit_Logger.start_audit(job_id=Job_Id,job_run_id=Run_Id,notebook_name=Notebook_Path_Name,source_layer="Raw",source_path_table=Source_Table,target_layer="Silver",target_table=Target_Table,operation_type="FullLoad",created_by=Executed_By)

            Source_Path = Source_Table  # now this is ADLS CSV path
            print(f"Reading CSV from ADLS path: {Source_Path}")
            Source_Df = read_source_csv_full(spark=spark,source_path=Source_Path)
            Source_Df_Count = Source_Df.count()
            if Source_Df_Count > 0:
                Source_Df = Source_Df.withColumn("ADB_LoadedTimestamp", current_timestamp())
              
                Validate_Schema = Data_Validation.schema_validation_fields_only(df=Source_Df,expected_fields=Target_Table_Fields)
                if Validate_Schema != "success":
                    Validate_Schema.display(truncate=False)
                    raise Exception("Fields not matching in source and target table")
                  
                Validate_Notnull = Data_Validation.validate_fields_no_nulls(df=Source_Df,fields=["ADB_LoadedTimestamp"])
                if Validate_Notnull != "success":
                    Validate_Notnull.display(truncate=False)
                    raise Exception("Fields are NULL")

                Records_Inserted, Records_Updated = Data_Process.perform_append_load_to_output(source_df_with_extra_fields=Source_Df,target_table=Target_Table_with_cat)

                Control_Manager.update_control_table(source_path_table=Source_Table,target_table=Target_Table,last_run_status="SUCCESS",records_processed=Source_Df_Count)
                Audit_Logger.end_audit(audit_id=Audit_Id,status="SUCCESS",records_read=Source_Df_Count,records_inserted=Records_Inserted,records_updated=Records_Updated,records_deleted=None,error_message=None)
                print(f"-> {Source_Table} is processed successfully")
            else:
                print("No new records to process")
                Control_Manager.update_control_table(source_path_table=Source_Table,target_table=Target_Table,last_run_status="SUCCESS",records_processed=0)
                Audit_Logger.end_audit(audit_id=Audit_Id,status="SUCCESS",records_read=0,records_inserted=0,records_updated=0,records_deleted=0,error_message="")
                print(f"-> {Source_Table} is having no new records to process")
except Exception as e:
    print(f"Load to table {Target_Table} failed: {str(e)}")
    print(traceback.format_exc())
    Error_Logger.log_error(job_id=Job_Id,job_run_id=Run_Id,notebook_name=Notebook_Path_Name,error_type=type(e).__name__,error_message=str(e),error_stack_trace=str(traceback.format_exc()),table_name=Target_Table,severity="ERROR",created_by=Executed_By)
    if Source_Table:
        Control_Manager.update_control_table(source_path_table=Source_Table,target_table=Target_Table,last_run_status="FAILED",records_processed=0)
    if Audit_Id:
        Audit_Logger.end_audit(audit_id=Audit_Id,status="FAILED",records_read=0,records_inserted=0,records_updated=0,records_deleted=0,error_message=str(e))
    raise
  
