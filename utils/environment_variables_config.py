# Databricks notebook source
# DBTITLE 1,Import Audit File
# MAGIC %run ../utils/audit_logger

# COMMAND ----------

# DBTITLE 1,Import Control File
# MAGIC %run ../utils/control_table_manager

# COMMAND ----------

# DBTITLE 1,Import Error Log File
# MAGIC %run ../utils/error_handler

# COMMAND ----------

# DBTITLE 1,Import Validation File
# MAGIC %run ../utils/data_validation_rule

# COMMAND ----------

# DBTITLE 1,Import DataProcessFunction File
# MAGIC %run ../utils/data_process_functions

# COMMAND ----------

# Get workspace information
try:
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().toString()
except Exception as e:
    raise RuntimeError(f"Failed to retrieve workspace details from Databricks context: {str(e)}")

# Check workspace url to determine environment
if "adb-123456789.15.azuredatabricks.net" in workspace_url:
    environment = "development"
    landing_layer_eo = "abfss://landing-eo@stdfcdevlndeo.dfs.core.windows.net/"
    landing_layer_jo = "abfss://landing-jo@stdfcdevlndjo.dfs.core.windows.net/"
    catalog_output_eo = "cat_dfc_dev_otp_eo"
    catalog_output_jo = "cat_dfc_dev_otp_jo"
    catalog_usecase = "cat_dfc_dev_uc"
    catalog_system = "cat_dfc_dev_system"

elif "adb-123456789.azuredatabricks.net" in workspace_url:
    environment = "production"
    landing_layer_eo = ""
    landing_layer_jo = ""
    catalog_output_eo = "cat_dfc_prod_otp_eo"
    catalog_output_jo = "cat_dfc_prod_otp_jo"
    catalog_usecase = "cat_dfc_prod_uc"
    catalog_system = "cat_dfc_prod_system"

else:
    environment = "Error"

print(f"Workspace URL: {workspace_url} - {environment}")

# Stop execution if environment is Error
if environment == "Error":
    raise SystemExit("ERROR: Databricks workspace does not match expected environments. Stopping execution.")
