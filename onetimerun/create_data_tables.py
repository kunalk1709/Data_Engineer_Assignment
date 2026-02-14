# Databricks notebook source
# DBTITLE 1, Import Util Files
# MAGIC %run ../utils/Environment_Variables_Config

# COMMAND ----------

# DBTITLE 1, Import Modules
import traceback
from pyspark.sql.functions import current_timestamp, date_format

# COMMAND ----------

# DBTITLE 1, Variable Set
Catalog_System = catalog_system
Catalog_Data = catalog_data

print(f"Catalogs: {Catalog_Data}, {Catalog_System}")

# COMMAND ----------
# DBTITLE 1
# Create dimension data tables
def create_customer_table():
    try:
        table_name = f"{Catalog_Data}.dim_data.tbl_dim_customer"
        print(f"Checking if dimension customer table exists: {table_name}")
        if spark.catalog.tableExists(table_name):
            print(f"Dimension table '{table_name}' already exists. Skipping creation.")
            return False
        print(f"Creating Dimension table: {table_name}")
        sql_script = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            customer_id STRING NOT NULL, customer_name STRING, email STRING, country STRING, city STRING, registration_date DATE, customer_segment STRING, ADB_LoadedTimestamp TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES ( delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
        """
        spark.sql(sql_script)
        print(f"Master table {table_name} created successfully")
        return True
    except Exception as e:
        print(f"Error creating master table: {str(e)}")
        print(traceback.format_exc())
        raise

def create_product_table():
    try:
        table_name = f"{Catalog_Data}.dim_data.tbl_dim_product"
        print(f"Checking if dimension table exists: {table_name}")
        if spark.catalog.tableExists(table_name):
            print(f"Dimension table '{table_name}' already exists. Skipping creation.")
            return False
        print(f"Creating Dimension table: {table_name}")
        sql_script = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            product_id STRING NOT NULL, product_name STRING, category STRING, brand STRING, cost_price DECIMAL(10, 2), list_price DECIMAL(10, 2), ADB_LoadedTimestamp TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES ( delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
        """
        spark.sql(sql_script)
        print(f"Dimension table {table_name} created successfully")
        return True
    except Exception as e:
        print(f"Error creating dimension table: {str(e)}")
        print(traceback.format_exc())
        raise

# Create Fact data tables
def create_order_table():
    try:
        table_name = f"{Catalog_Data}.fact_data.tbl_fact_order"
        print(f"Checking if fact table exists: {table_name}")
        if spark.catalog.tableExists(table_name):
            print(f"Fact table '{table_name}' already exists. Skipping creation.")
            return False
        print(f"Creating Fact table: {table_name}")
        sql_script = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            order_id STRING NOT NULL, customer_id STRING, order_date DATE, product_id STRING, quantity int, unit_price DECIMAL(10, 2), status STRING, payment_method STRING, ADB_LoadedTimestamp TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES ( delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
        """
        spark.sql(sql_script)
        print(f"Fact table {table_name} created successfully")
        return True
    except Exception as e:
        print(f"Error creating fact table: {str(e)}")
        print(traceback.format_exc())
        raise


# DBTITLE 1, Function Execution
try:
    create_order_table()
    create_product_table()
    create_customer_table()
except Exception as e:
    print(f"Error: {str(e)}")
    print(traceback.format_exc())
    raise

# Insert initial table created record into system control table
import traceback
try:
    Control_Table = f"{Catalog_System}.dfc_system_poc.tbl_sys_control"
    sql_script = f"""
    INSERT INTO {Control_Table}
    (source_layer,source_path_table,target_layer,target_table,target_table_type,load_type,last_run_timestamp,last_run_status,last_watermark,records_processed,is_active,created_timestamp,updated_timestamp)
    SELECT *
    FROM (
        SELECT 'Raw' AS source_layer, 'abfss://raw@adlsacc.dfs.core.windows.net/dim_data/customers.csv' AS source_path_table,'Silver' AS target_layer,'dim_data.tbl_dim_customer' AS target_table,
            'Dim' AS target_table_type,'FullLoad' AS load_type,current_timestamp() AS last_run_timestamp,'Not_Run' AS last_run_status,NULL AS last_watermark,NULL AS records_processed,'Y' AS is_active,
            current_timestamp() AS created_timestamp,current_timestamp() AS updated_timestamp
        UNION ALL
        SELECT 'Raw' AS source_layer,'abfss://raw@adlsacc.dfs.core.windows.net/dim_data/products.csv' AS source_path_table,'Silver' AS target_layer,'dim_data.tbl_dim_product' AS target_table,
            'Dim' AS target_table_type,'FullLoad' AS load_type,current_timestamp() AS last_run_timestamp,'Not_Run' AS last_run_status,NULL AS last_watermark,NULL AS records_processed,'Y' AS is_active,
            current_timestamp() AS created_timestamp,current_timestamp() AS updated_timestamp
        UNION ALL
        SELECT 'Raw' AS source_layer,'abfss://raw@adlsacc.dfs.core.windows.net/fact_data/orders.csv' AS source_path_table,'Silver' AS target_layer,'fact_data.tbl_fact_order' AS target_table,
            'Fact' AS target_table_type,'IncrementalLoad' AS load_type,current_timestamp() AS last_run_timestamp,'Not_Run' AS last_run_status,NULL AS last_watermark,NULL AS records_processed,'Y' AS is_active,
            current_timestamp() AS created_timestamp,current_timestamp() AS updated_timestamp
        UNION ALL
        SELECT 'Raw' AS source_layer,'abfss://raw@adlsacc.dfs.core.windows.net/fact_data/events.csv' AS source_path_table,'Silver' AS target_layer,'fact_data.tbl_fact_event' AS target_table,
            'Fact' AS target_table_type,'IncrementalLoad' AS load_type,current_timestamp() AS last_run_timestamp,'Not_Run' AS last_run_status,NULL AS last_watermark,NULL AS records_processed,'Y' AS is_active,
            current_timestamp() AS created_timestamp,current_timestamp() AS updated_timestamp
    ) AS new_records
    WHERE concat(new_records.source_path_table, '||', new_records.target_table) NOT IN ( SELECT concat(c.source_path_table, '||', c.target_table) FROM {Control_Table} c )
    """
    spark.sql(sql_script)

    sql_script_1 = f"""
    SELECT concat(source_path_table, '||', target_table) FROM {Control_Table} WHERE date_format(created_timestamp, 'yyyy-MM-dd HH') = date_format(current_timestamp(), 'yyyy-MM-dd HH')
    """
    spark.sql(sql_script_1).display()
    print(f"Inserted records into system control table: {Control_Table}")
except Exception as e:
    print(f"Error: {str(e)}")
    print(traceback.format_exc())
    raise
