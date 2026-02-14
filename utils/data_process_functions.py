# Databricks notebook source
# DBTITLE 1, Import Modules
import os
import traceback
import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, date_format, col
from pyspark.sql.types import (StructType,StructField,IntegerType,StringType,TimestampType,ArrayType,MapType)
from typing import Literal
from typing_extensions import Literal as TypingLiteral
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1, DataProcess Class with Functions
class DataProcess:
    # A data process utility having functions being used for data processing

    def __init__(self, spark):
        self.spark = spark

    # Read source data (incremental/full)
    def read_source_data_table(self,last_watermark: str,source_table: str,watermark_col: str) -> DataFrame:
        # Reads data from landing layer with optional incremental processing
        try:
            print(f"-> Reading data from: {source_table}")

            if last_watermark:
                print(f"-> Incremental load - Last watermark: {last_watermark}")
                source_df = (self.spark.read.table(source_table).filter(col(watermark_col) > last_watermark))
            else:
                print("-> Full load - No watermark found")
                source_df = self.spark.read.table(source_table)

            return source_df
        except Exception as e:
            print(f"Error reading source data: {str(e)}")
            raise

    def read_source_data_table_full(self, source_table: str) -> DataFrame:
        # Reads full data from landing layer
        try:
            print(f"-> Reading data from: {source_table}")
            print("-> Full load")
            return self.spark.read.table(source_table)
        except Exception as e:
            print(f"Error reading source data: {str(e)}")
            raise

    def read_source_csv_full(spark: SparkSession,source_path: str,header: bool = True,infer_schema: bool = True,delimiter: str = ",") -> DataFrame:
        return (spark.read.format("csv").option("header", header).option("inferSchema", infer_schema).option("delimiter", delimiter).load(source_path))

    def read_source_json_full(spark: SparkSession,source_path: str,multiline: bool = True) -> DataFrame:
    return (spark.read.format("json").option("multiline", multiline).load(source_path))

    def create_target_table_from_json(spark: SparkSession,df: DataFrame,target_table_with_cat: str):
        print(f"Creating target table {target_table_with_cat} from JSON schema")
        df.limit(0).write.format("delta").mode("overwrite").saveAsTable(target_table_with_cat)

    # UPSERT (MERGE) to output
    def perform_upsert_to_output(self,source_df_with_extra_fields: DataFrame,target_table: str,merge_key: str) -> tuple[int, int]:
        # Performs upsert operation from source to output layer
        try:
            print(f"-> Starting upsert to: {target_table}")

            target_exists = self.spark.catalog.tableExists(target_table)
            if not target_exists:
                raise Exception(f"Target table {target_table} does not exist")

            target_delta = DeltaTable.forName(self.spark, target_table)

            merge_result = target_delta.alias("target").merge(
                    source_df_with_extra_fields.alias("source"),
                    f"target.{merge_key} = source.{merge_key}" )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute() 

            # Read Delta metrics
            last_op = target_delta.history(1)
            metrics_row = last_op.select("operationMetrics").collect()[0][0]
            records_inserted = int(metrics_row.get("numTargetRowsInserted", "0"))
            records_updated = int(metrics_row.get("numTargetRowsUpdated", "0"))

            print(f"-> Inserted {records_inserted} records and updated {records_updated} records to {target_table}")
            return records_inserted, records_updated
        except Exception as e:
            print(f"Error during upsert: {str(e)}")
            raise

    # APPEND (FULL LOAD) to output
    def perform_append_load_to_output(self,source_df_with_extra_fields: DataFrame,target_table: str) -> tuple[int, int]:
        #Performs append operation from source to output layer
        try:
            print(f"-> Starting full load to: {target_table}")
            target_exists = self.spark.catalog.tableExists(target_table)
            if not target_exists:
                raise Exception(f"Target table {target_table} does not exist")

            source_df_with_extra_fields.write.format("delta").mode("append").saveAsTable(target_table)
            target_delta = DeltaTable.forName(self.spark, target_table)
            last_op = target_delta.history(1)
            metrics_row = last_op.select("operationMetrics").collect()[0][0]
            records_inserted = int(metrics_row.get("numOutputRows", "0"))
            records_updated = 0
            print(f"-> Inserted {records_inserted} records and updated {records_updated} records to {target_table}")
            return records_inserted, records_updated

        except Exception as e:
            print(f"Error during upsert: {str(e)}")
            raise

    # Watermark calculation
    def get_new_watermark(self,source_df: DataFrame,watermark_col: str) -> str | Literal["source_df empty"]:
        # Calculates new watermark from source data
        try:
            if source_df.count() > 0:
                max_timestamp = source_df.agg({watermark_col: "max"}).collect()[0][0]
                print(f"-> New watermark: {max_timestamp}")
                return max_timestamp
            else:
                return "source_df empty"
        except Exception as e:
            print(f"Error fetching watermark: {str(e)}")
            raise
          
print("Data Process Utility Imported")

# COMMAND ----------

# DBTITLE 1, Test Function
# import os
# import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import (StructType,StructField,IntegerType,StringType,TimestampType)

# data_process = DataProcess(spark)
# source_df = data_process.read_source_data_table('2025-12-10T02:34:40.000','edh_unreg_gold_run.essent_s4u.acdoca','TIMESTAMP')

# records_inserted, records_updated = data_process.perform_upsert_to_output(source_df,'target_table_name','merge_key')

# new_watermark = data_process.get_new_watermark(source_df,'TIMESTAMP')
