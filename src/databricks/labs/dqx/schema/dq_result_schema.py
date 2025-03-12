from pyspark.sql.types import StructType, StructField, ArrayType, StringType, TimestampType

dq_result_schema = ArrayType(
    StructType(
        [
            StructField("name", StringType(), nullable=True),
            StructField("message", StringType(), nullable=True),
            StructField("col_name", StringType(), nullable=True),
            StructField("filter", StringType(), nullable=True),
            StructField("function", StringType(), nullable=True),
            StructField("run_time", TimestampType(), nullable=True),
        ]
    )
)
