# Databricks notebook source
# DBTITLE 1, Import Util Files
# MAGIC %run ../utils/Environment_Variables_Config

# COMMAND ----------
# DBTITLE 1, Import Modules
import os
import traceback
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (current_timestamp, date_format, col, lit, sha2, concat_ws,coalesce, when, to_timestamp, current_date, to_date,regexp_extract, split, instr)
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,TimestampType, ArrayType, MapType)
from typing import Literal
from typing_extensions import Literal
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

# COMMAND ----------
# DBTITLE 1, Variable_Set / Initialize_Utilities / Job_Metadata
# Input from Job UI if require to process again after today's run
try:
    Today_Process_Rerun = dbutils.widgets.get("Today_Process_Rerun")
except Exception:
    dbutils.widgets.text("Today_Process_Rerun", "FALSE", "Today Process Rerun")
    Today_Process_Rerun = dbutils.widgets.get("Today_Process_Rerun")

Catalog_System = catalog_system
Catalog_Data = catalog_data

Table_Audit = f"{Catalog_System}.dfc_system_poc.tbl_sys_audit"
Table_Control = f"{Catalog_System}.dfc_system_poc.tbl_sys_control"
Table_Error = f"{Catalog_System}.dfc_system_poc.tbl_sys_error"

print(f"Catalogs: {Catalog_Data}, {Catalog_System}")
print(f"System Tables: {Table_Audit}, {Table_Control}, {Table_Error}")

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
# DBTITLE 1, Load Order Fact Table

Target_Table = "fact_data.tbl_fact_order"
Target_Table_with_cat = f"{Catalog_Data}.fact_data.tbl_fact_order"
try:
    Audit_Id = None
    Target_Table_Fields = spark.table(Target_Table_with_cat).columns
    # Get list of all source tables for Output layer JO ACDOCA table
    df = spark.table(Table_Control).filter(col("target_table") == Target_Table).select(col("source_path_table").alias("source_table")).dropna().distinct()
    df.display(truncate=False)

    for src in df.collect():
        Source_Table = src["source_table"]
        print(f"source_table: {Source_Table}")

        # Check if combination of target table and source table is active
        if not Control_Manager.is_table_active(target_table=Target_Table,source_path_table=Source_Table):
            raise Exception(f"Table {Target_Table} for source {Source_Table} is Inactive in control table")
        print(f"-> target table {Target_Table} for source {Source_Table} is active in control table {Table_Control}")

        # Check if data is already loaded today
        df_check_source_target_loaded_today = (spark.table(Table_Control).filter(
                (col("source_path_table") == Source_Table) & (col("target_table") == Target_Table) &
                (to_date(col("last_run_timestamp")) == current_date()) & (col("last_run_status") == "SUCCESS") ) )

        if df_check_source_target_loaded_today.count() > 0 and Today_Process_Rerun == "FALSE":
            print(f"-> Data is already loaded for {Source_Table} for today. Skipping.")
            continue

        # Start audit logging
        Audit_Id = Audit_Logger.start_audit(job_id=Job_Id,job_run_id=Run_Id,notebook_name=Notebook_Path_Name,source_layer="Raw",source_path_table=Source_Table,target_layer="Output",
            target_table=Target_Table,operation_type="Upsert",created_by=Executed_By)

        # Get last watermark
        Last_Watermark = Control_Manager.get_last_watermark(target_table=Target_Table,source_path_table=Source_Table)
        print(f"-> last watermark: {Last_Watermark}")

        # Read raw data
        Last_Watermark = (datetime.strptime(str(Last_Watermark), "%Y-%m-%d %H:%M:%S").replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S"))

        Source_Path = Source_Table  # now this is ADLS CSV path
        print(f"Reading CSV from ADLS path: {Source_Path}")
        Source_Df = read_source_csv_full(spark=spark,source_path=Source_Path)
        Source_Df = Source_Df.filter(to_date(col("order_date")) < current_date() & (to_date(col("order_date")) >= lit(Last_Watermark)))
        Source_Df_Count = Source_Df.count()

        if Source_Df_Count > 0:
            Source_Df_1 = Source_Df.withColumn("ADB_LoadedTimestamp", current_timestamp())

            # Schema validation
            Validate_Schema = Data_Validation.schema_validation_fields_only(df=Source_Df_With_Target_Fields,expected_fields=Target_Table_Fields)
            if Validate_Schema != "success":
                Validate_Schema.display(truncate=False)
                raise Exception("Fields not matching in source and target table")

            # Uniqueness validation
            Validate_Uniqueness = Data_Validation.validate_fields_unique(df=Source_Df_With_Target_Fields,fields=["order_id"])
            if Validate_Uniqueness != "success":
                Validate_Uniqueness.display(truncate=False)
                raise Exception("Composite Field Values are not Unique")

            # Validate not null for each of few fields
            Validate_Notnull = Data_Validation.validate_fields_no_nulls(df=Source_Df_With_Target_Fields,fields=["order_id", "customer_id", "product_id"])
            if Validate_Notnull == "success":
                print("-> Fields are not NULL")
            else:
                # Here, type checkers know 'result' is a DataFrame
                Validate_Notnull.display(truncate=False)
                raise Exception("Fields are NULL")

            # Perform upsert
            Records_Inserted, Records_Updated = Data_Process.perform_upsert_to_output(source_df_with_extra_fields=Source_Df_With_Target_Fields,target_table=Target_Table_with_cat,merge_key="order_id")
            
            # Calculate new watermark
            New_Watermark = Data_Process.get_new_watermark(source_df=Source_Df_With_Target_Fields,watermark_col="order_date")
            New_Watermark = f"{New_Watermark}"
            
            # Update control table
            Control_Manager.update_control_table(source_path_table=Source_Table,target_table=Target_Table,last_run_status="SUCCESS",records_processed=Source_Df_Count,new_watermark=New_Watermark)
            
            # End audit logging
            Audit_Logger.end_audit(audit_id=Audit_Id,status="SUCCESS",records_read=Source_Df_Count,records_inserted=Records_Inserted,records_updated=Records_Updated,records_deleted=None,error_message=None)
            print(f"-> {Source_Table} is processed successfully")

        else:
            print("No new records to process")
            # Update control table
            Control_Manager.update_control_table(source_path_table=Source_Table,target_table=Target_Table,last_run_status="SUCCESS",records_processed=0)
            # End audit logging
            Audit_Logger.end_audit(audit_id=Audit_Id,status="SUCCESS",records_read=0,records_inserted=0,records_updated=0,records_deleted=0,error_message="")
            print(f"-> {Source_Table} is having no new records to process")

except Exception as e:
    print(f"Load to table {Target_Table} failed: {str(e)}")
    print(traceback.format_exc())
    # Log error
    Error_Logger.log_error(job_id=Job_Id,job_run_id=Run_Id,notebook_name=Notebook_Path_Name,error_type=type(e).__name__,error_message=str(e),
        error_stack_trace=str(traceback.format_exc()),table_name=Target_Table,severity="ERROR",created_by=Executed_By)
    # Update control table with failure
    Control_Manager.update_control_table(source_path_table=Source_Table,target_table=Target_Table,last_run_status="FAILED",records_processed=0)
    # End audit logging with failure
    if Audit_Id:
        Audit_Logger.end_audit(audit_id=Audit_Id,status="FAILED",records_read=0,records_inserted=0,records_updated=0,records_deleted=0,error_message=str(e))
    raise

# COMMAND ----------

# DBTITLE 1, Optimize Order table
from delta.tables import DeltaTable
full_name = f"{Catalog_Data}.fact_data.tbl_fact_order"
dt = DeltaTable.forName(spark, full_name)
dt.vacuum(360) # 15 days
dt.optimize().executeZorderBy(["order_id"])


# COMMAND ----------
# DBTITLE 1, Load Event Fact Table
Target_Table = "fact_data.tbl_fact_event"
Target_Table_with_cat = f"{Catalog_Data}.{Target_Table}"
try:
    Audit_Id = None
    # Get all source paths
    df_sources = spark.table(Table_Control).filter(col("target_table") == Target_Table).select(col("source_path_table").alias("source_table")).dropna().distinct())
    df_sources.display(truncate=False)
    for src in df_sources.collect():
        Source_Table = src["source_table"]
        print(f"\nProcessing source: {Source_Table}")

        # Check active flag
        if not Control_Manager.is_table_active(target_table=Target_Table,source_path_table=Source_Table):
            raise Exception(f"Source {Source_Table} is inactive")

        # Skip if already processed today
        already_loaded = spark.table(Table_Control).filter(
                (col("source_path_table") == Source_Table) & (col("target_table") == Target_Table) & (to_date(col("last_run_timestamp")) == current_date()) &
                (col("last_run_status") == "SUCCESS")).count()
        if already_loaded > 0 and Today_Process_Rerun == "FALSE":
            print("Already processed today. Skipping.")
            continue

        # Start audit
        Audit_Id = Audit_Logger.start_audit(job_id=Job_Id,job_run_id=Run_Id,notebook_name=Notebook_Path_Name,source_layer="Raw",source_path_table=Source_Table,
            target_layer="Output",target_table=Target_Table,operation_type="Upsert",created_by=Executed_By)

        # Get watermark
        Last_Watermark = Control_Manager.get_last_watermark(target_table=Target_Table,source_path_table=Source_Table)

        if Last_Watermark:
            Last_Watermark = (datetime.datetime.strptime(str(Last_Watermark), "%Y-%m-%d %H:%M:%S").replace(hour=0, minute=0, second=0))


        # Read source
        Source_Df = read_source_json_full(spark, Source_Table)

        # Create target table if not exists (JSON ONLY)
        if not spark.catalog.tableExists(Target_Table_with_cat):
          create_target_table_from_json(spark,Source_Df.withColumn("ADB_LoadedTimestamp", current_timestamp()),Target_Table_with_cat)

        # Watermark filter
        if Last_Watermark:
            Source_Df = Source_Df.filter(to_timestamp(col("timestamp")) >= lit(Last_Watermark))

        Source_Df_Count = Source_Df.count()
        if Source_Df_Count == 0:
            print("No new records")
            Control_Manager.update_control_table(source_path_table=Source_Table,target_table=Target_Table,last_run_status="SUCCESS",records_processed=0)
            Audit_Logger.end_audit(audit_id=Audit_Id,status="SUCCESS",records_read=0,records_inserted=0,records_updated=0,records_deleted=0,error_message="")
            continue

        # Add audit column
        Source_Df = Source_Df.withColumn("ADB_LoadedTimestamp", current_timestamp())

        # Align schema with target
        target_schema = spark.table(Target_Table_with_cat).schema
        target_fields = [f.name for f in target_schema]
        for field in target_schema:
            if field.name not in Source_Df.columns:
                Source_Df = Source_Df.withColumn(field.name, lit(None).cast(field.dataType))
        Source_Df_With_Target_Fields = Source_Df.select(*target_fields)

        # Validations
        Data_Validation.schema_validation_fields_only(Source_Df_With_Target_Fields,target_fields)
        Data_Validation.validate_fields_unique(Source_Df_With_Target_Fields,["event_id"])

        Data_Validation.validate_fields_no_nulls(Source_Df_With_Target_Fields,["event_id", "timestamp"])

        # Upsert
        Records_Inserted, Records_Updated = Data_Process.perform_upsert_to_output(source_df_with_extra_fields=Source_Df_With_Target_Fields,target_table=Target_Table_with_cat,merge_key="event_id")

        # New watermark
        New_Watermark = Data_Process.get_new_watermark(Source_Df_With_Target_Fields,"timestamp")

        # Update control
        Control_Manager.update_control_table(source_path_table=Source_Table,target_table=Target_Table,last_run_status="SUCCESS",records_processed=Source_Df_Count,new_watermark=str(New_Watermark))

        # End audit
        Audit_Logger.end_audit(audit_id=Audit_Id,status="SUCCESS",records_read=Source_Df_Count,records_inserted=Records_Inserted,records_updated=Records_Updated,records_deleted=None,error_message=None)
        print(f"SUCCESS: {Source_Table}")

except Exception as e:
    print(f"FAILED: {str(e)}")
    print(traceback.format_exc())
    Error_Logger.log_error(job_id=Job_Id,job_run_id=Run_Id,notebook_name=Notebook_Path_Name,error_type=type(e).__name__,error_message=str(e),error_stack_trace=str(traceback.format_exc()),table_name=Target_Table,severity="ERROR",created_by=Executed_By)
    if Audit_Id:
        Audit_Logger.end_audit(audit_id=Audit_Id,status="FAILED",records_read=0,records_inserted=0,records_updated=0,records_deleted=0,error_message=str(e))
    raise
