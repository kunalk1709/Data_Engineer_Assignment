# Databricks notebook source
# DBTITLE 1, Import Util Files
# MAGIC %run ../utils/Environment_Variables_Config

# COMMAND ----------

# DBTITLE 1, Import Modules
import traceback
# COMMAND ----------

# DBTITLE 1, Variable Set
Catalog_System = catalog_system
print(f"Catalog: {Catalog_System}")

# COMMAND ----------

# DBTITLE 1, Func Create Audit Log Table
# Create Audit log table for tracking job executions
def create_audit_table():
    try:
        table_name = f"{Catalog_System}.dfc_system_poc.tbl_sys_audit"
        print(f"Checking if audit table exists: {table_name}")
        # Check if the table already exists
        if spark.catalog.tableExists(table_name):
            print(f"Audit table '{table_name}' already exists. Skipping creation.")
            return False
        print(f"Creating audit table: {table_name}")
        sql_script = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                audit_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                job_id STRING NOT NULL, job_run_id STRING, notebook_name STRING NOT NULL, source_layer STRING NOT NULL, source_path_table STRING,
                target_layer STRING NOT NULL, target_table STRING NOT NULL, operation_type STRING NOT NULL, records_read BIGINT, records_inserted BIGINT,
                records_updated BIGINT, records_deleted BIGINT, status STRING NOT NULL, start_time TIMESTAMP, end_time TIMESTAMP, error_message STRING, created_by STRING NOT NULL,
                CONSTRAINT pk_audit PRIMARY KEY (audit_id)
            )
            USING DELTA
            TBLPROPERTIES ( delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true )
        """
        spark.sql(sql_script)
        print(f"Audit table {table_name} created successfully")
        return True
    except Exception as e:
        print(f"Error creating audit table: {str(e)}")
        print(traceback.format_exc())
        raise

# COMMAND ----------

# DBTITLE 1, Func Create Control Metadata Table
# Create control table for managing pipeline metadata
def create_control_table():
    try:
        table_name = f"{Catalog_System}.dfc_system_poc.tbl_sys_control"
        print(f"Checking if control table exists: {table_name}")
        # Check if the table already exists
        if spark.catalog.tableExists(table_name):
            print(f"Control table '{table_name}' already exists. Skipping creation.")
            return False
        print(f"Creating control table: {table_name}")
        sql_script = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                control_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                source_layer STRING, source_path_table STRING, target_layer STRING, target_table STRING NOT NULL, target_table_type STRING,
                load_type STRING, last_run_timestamp TIMESTAMP NOT NULL, last_run_status STRING NOT NULL, last_watermark TIMESTAMP_NTZ, records_processed BIGINT,
                is_active STRING NOT NULL, created_timestamp TIMESTAMP NOT NULL, updated_timestamp TIMESTAMP NOT NULL,
                CONSTRAINT pk_control PRIMARY KEY (control_id)
            )
            USING DELTA
            TBLPROPERTIES ( delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true )
        """
        spark.sql(sql_script)
        print(f"Control table {table_name} created successfully")
        return True
    except Exception as e:
        print(f"Error creating control table: {str(e)}")
        print(traceback.format_exc())
        raise

# COMMAND ----------

# DBTITLE 1, Func Create Error Log Table
# Creates error log table for tracking errors
def create_error_log_table():
    try:
        table_name = f"{Catalog_System}.dfc_system_poc.tbl_sys_error"
        print(f"Checking if error table exists: {table_name}")
        # Check if the table already exists
        if spark.catalog.tableExists(table_name):
            print(f"Error table '{table_name}' already exists. Skipping creation.")
            return False
        print(f"Creating error log table: {table_name}")
        sql_script = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                error_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                job_id STRING NOT NULL, job_run_id STRING, notebook_name STRING NOT NULL,
                error_type STRING NOT NULL,   /* class of error or exception */
                error_message STRING NOT NULL, error_stack_trace STRING, table_name STRING, severity STRING NOT NULL,
                created_timestamp TIMESTAMP NOT NULL, created_by STRING NOT NULL,
                CONSTRAINT pk_error PRIMARY KEY (error_id)
            )
            USING DELTA
            TBLPROPERTIES (delta.enableChangeDataFeed = true,delta.autoOptimize.optimizeWrite = true,delta.autoOptimize.autoCompact = true)
        """
        spark.sql(sql_script)
        print(f"Error log table {Catalog_System}.dfc_system_poc.tbl_sys_error created successfully")
        return True
    except Exception as e:
        print(f"Error creating error log table: {str(e)}")
        print(traceback.format_exc())
        raise

# COMMAND ----------

# DBTITLE 1, Func Execution
try:
    create_audit_table()
    create_control_table()
    create_error_log_table()
except Exception as e:
    print(f"Error: {str(e)}")
    print(traceback.format_exc())
    raise
