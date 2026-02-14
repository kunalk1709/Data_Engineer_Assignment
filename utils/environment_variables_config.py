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
    catalog_data = "cat_data"
    catalog_system = "cat_dfc_dev_system"
elif "adb-123456789.azuredatabricks.net" in workspace_url:
    environment = "production"
    catalog_data = "cat_data"
    catalog_system = "cat_dfc_prod_system"
else:
    environment = "Error"
print(f"Workspace URL: {workspace_url} - {environment}")

# Stop execution if environment is Error
if environment == "Error":
    raise SystemExit("ERROR: Databricks workspace does not match expected environments. Stopping execution.")
