# Databricks notebook source
# DBTITLE 1, Import Modules

import os
import traceback
from typing import Literal
from typing_extensions import Literal as TypingLiteral

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, date_format
from pyspark.sql.types import (StructType,StructField,StringType,ArrayType,MapType)

# COMMAND ----------

# DBTITLE 1, DataValidation Class with schema_validation, validate_fields_unique & validate_fields_no_nulls

class DataValidation:
    """A data validation utility for Databricks / PySpark DataFrames"""
    def __init__(self, spark):
        self.spark = spark
      
    # Helper utilities
    @staticmethod
    def _normalize_dtype(dtype_str: str) -> str:
        """Normalize type strings to Spark simpleString format (case-insensitive)."""
        return (dtype_str or "").strip().lower()
    @staticmethod
    def _df_schema_as_dict(df: DataFrame) -> dict:
        """Extract DataFrame schema as {column_name: simpleString_type}."""
        return {
            field.name: field.dataType.simpleString().lower()
            for field in df.schema.fields
        }
      
    # Schema validation (columns + types)
    def schema_validation(self, df: DataFrame, expected_schema: dict[str, str]) -> DataFrame | Literal["success"]:
        """
        Validate df columns and types against expected_schema.
        """
        try:
            expected = {col: self._normalize_dtype(t) for col, t in (expected_schema or {}).items()}
            actual = self._df_schema_as_dict(df)

            expected_cols = set(expected.keys())
            actual_cols = set(actual.keys())

            missing_fields = sorted(list(expected_cols - actual_cols))
            unexpected_fields = sorted(list(actual_cols - expected_cols))
            # Fields present in both
            common = expected_cols & actual_cols

            matched_fields: list[str] = []
            mismatched_types: list[dict[str, str]] = []

            for col in sorted(common):
                if expected[col] == actual[col]:
                    matched_fields.append(col)
                else:
                    # Collect mismatch records as a list of dicts
                    mismatched_types.append({"field": col,"expected": expected[col],"actual": actual[col]})

            if (len(missing_fields) == 0 and len(unexpected_fields) == 0 and len(mismatched_types) == 0):
                return "success"
            else:
                result = {
                    "matched_fields": matched_fields,
                    "missing_fields": missing_fields,
                    "unexpected_fields": unexpected_fields,
                    "mismatched_types": mismatched_types,
                    "expected_schema": expected,
                    "actual_schema": actual
                }
    
                result_schema = StructType([
                    StructField("matched_fields", ArrayType(StringType()), nullable=True),
                    StructField("missing_fields", ArrayType(StringType()), nullable=True),
                    StructField("unexpected_fields", ArrayType(StringType()), nullable=True),
                    StructField("mismatched_types",ArrayType(
                            StructType([
                                StructField("field", StringType(), nullable=True),
                                StructField("expected", StringType(), nullable=True),
                                StructField("actual", StringType(), nullable=True)
                            ]) ), nullable=True),
                    StructField("expected_schema", MapType(StringType(), StringType()), nullable=True),
                    StructField("actual_schema", MapType(StringType(), StringType()), nullable=True)
                ])

            return self.spark.createDataFrame([result], schema=result_schema)

        except Exception as e:
            print(f"Critical schema validation failure: {str(e)}")
            print(traceback.format_exc())
            raise

    # Schema validation (fields only)
    def schema_validation_fields_only(self,df: DataFrame,expected_fields: list[str]) -> DataFrame | Literal["success"]:
        """
        Validate df columns against expected_fields only.
        """
        try:
            # Normalize expected and actual
            exp_list_raw = list(expected_fields or [])
            act_list_raw = list(df.columns or [])
          
            exp_list = [c.strip().lower() for c in exp_list_raw]
            act_list = [c.strip().lower() for c in act_list_raw]
          
            # Sets for presence checks (orders-insensitive)
            exp_set = set(exp_list)
            act_set = set(act_list)

            missing_fields = sorted(list(exp_set - act_set))
            unexpected_fields = sorted(list(act_set - exp_set))
            matched_fields = sorted(list(exp_set & act_set))

            if len(missing_fields) == 0 and len(unexpected_fields) == 0:
                return "success"

            else:
                # Build result payload, also include original (non-normalized) fields for clarity
                result = {
                    "matched_fields": matched_fields,
                    "missing_fields": missing_fields,
                    "unexpected_fields": unexpected_fields,
                    "expected_fields": sorted(list(exp_set)),
                    "actual_fields": sorted(list(act_set))
                }
    
                result_schema = StructType([
                    StructField("matched_fields", ArrayType(StringType()), nullable=True),
                    StructField("missing_fields", ArrayType(StringType()), nullable=True),
                    StructField("unexpected_fields", ArrayType(StringType()), nullable=True),
                    StructField("expected_fields", ArrayType(StringType()), nullable=True),
                    StructField("actual_fields", ArrayType(StringType()), nullable=True)
                ])
    
                return self.spark.createDataFrame([result], schema=result_schema)

        except Exception as e:
            print(f"Critical schema fields validation failure: {str(e)}")
            print(traceback.format_exc())
            raise

    # -------------------------------
    # Composite uniqueness validation
    # -------------------------------

    def validate_fields_unique(self,df: DataFrame,fields: list[str]) -> DataFrame | TypingLiteral["success"]:
        """
        Validate that the combination of fields is unique in df.
        """
        try:
            if not fields or df.limit(1).count() == 0:
                # No fields to check, treat as success
                return "success"

            missing = [c for c in fields if c not in df.columns]
            if missing:
                raise ValueError(f"Missing columns in DataFrame: {missing}")

            # Group by composite key and find duplicates (count > 1)
            dup_df = (
                df.groupBy(*fields)
                  .agg(F.count(F.lit(1)).alias("dup_count"))
                  .filter(F.col("dup_count") > 1)
                  .orderBy(F.col("dup_count").desc())
            )
            # If no duplicates, return "success", else return the duplicates DF
            if dup_df.limit(1).count() == 0:
                return "success"
            return dup_df

        except Exception as e:
            print(f"Critical composite unique validation failure: {str(e)}")
            print(traceback.format_exc())
            raise

    # NULL validation
    def validate_fields_no_nulls(self,df: DataFrame,fields: list[str]) -> DataFrame | TypingLiteral["success"]:
        """
        Validate that specified fields contain no NULL values.
        """
        try:
            if not fields or df.limit(1).count() == 0:
                return "success"

            missing = [c for c in fields if c not in df.columns]
            if missing:
                raise ValueError(f"Missing columns in DataFrame: {missing}")

            # Build expressions to count NULLs per field
            null_count_exprs = [
                F.sum(F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(c)
                for c in fields
            ]
            # Compute null counts for all fields in a single pass
            counts_row = df.agg(*null_count_exprs)

            # Convert wide row to long format: field_name, null_count
            # Using stack to reshape the single-row aggregate into rows per field 
            n = len(fields)
            stack_args = ", ".join([f"'{c}', `{c}`" for c in fields])
            nulls_df = (
                counts_row.selectExpr(f"stack({n}, {stack_args}) as (field_name, null_count)")
                .filter(F.col("null_count") > 0)
                .orderBy(F.col("null_count").desc())
            )

            # If no fields have nulls, return success; else return the nulls DF
            if nulls_df.limit(1).count() == 0:
                return "success"
            return nulls_df

        except Exception as e:
            print(f"Critical null-check validation failure: {str(e)}")
            print(traceback.format_exc())
            raise

print("Data Validation Utility Imported")

# COMMAND ----------

# DBTITLE 1, Test Function

# import os
# import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import (StructType,StructField,IntegerType,StringType,TimestampType)

# # Create sample DataFrame for testing
# schema = StructType([
#     StructField("id", IntegerType(), nullable=False),
#     StructField("name", StringType(), nullable=True),
#     StructField("created_date", TimestampType(), nullable=True)
# ])
# data = [(1, "John Doe", datetime.datetime.now()),(2, "Mark Dew", datetime.datetime.now())]
# df = spark.createDataFrame(data, schema)
# df.show(truncate=False)

# # Initialize validation utility
# data_validation = DataValidation(spark)

# # Schema validation (types)
# expected_schema = {"id": "int","name": "string","created_date": "timestamp"}
# validate_schema = data_validation.schema_validation(df, expected_schema)
# if validate_schema == "success":
#     print("Schema is valid")
# else:
#     validate_schema.display(truncate=False)
#     raise Exception("Schema is invalid")

# # Composite uniqueness check
# fields_list = ["id", "name"]
# validate_fields_unique = data_validation.validate_fields_unique(df, fields_list)
# if validate_fields_unique == "success":
#     print("Composite Field Values are Unique")
# else:
#     validate_fields_unique.display(truncate=False)
#     raise Exception("Composite Field Values are not Unique")

# # NULL validation
# fields_list = ["name", "created_date"]
# validate_fields_no_nulls = data_validation.validate_fields_no_nulls(df, fields_list)
# if validate_fields_no_nulls == "success":
#     print("Fields are not NULL")
# else:
#     validate_fields_no_nulls.display(truncate=False)
#     raise Exception("Fields contain NULL values")
