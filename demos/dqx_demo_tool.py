# Databricks notebook source
# MAGIC %md
# MAGIC # Demonstrate DQX usage when installed in the workspace
# MAGIC ### Installation of DQX in the workspace
# MAGIC
# MAGIC Install DQX in the workspace using default user installation as per the instructions [here](https://github.com/databrickslabs/dqx?tab=readme-ov-file#installation).
# MAGIC
# MAGIC Run in your terminal: `databricks labs install dqx`
# MAGIC
# MAGIC When prompt provide the following:
# MAGIC * Input data location: `/databricks-datasets/delta-sharing/samples/nyctaxi_2019`
# MAGIC * Input format: `delta`
# MAGIC * Output table: skip (not used as part of this demo)
# MAGIC * Quarantined table location: valid fully qualified Unity Catalog name (catalog.schema.table). The quarantined data will be saved there as part of this demo.
# MAGIC * Filename for data quality rules (checks): use default (`checks.yml`)
# MAGIC * Filename for profile summary statistics: use default (`profile_summary_stats.yml`)
# MAGIC
# MAGIC You can open the config and update it if needed after the installation.
# MAGIC
# MAGIC Run in your terminal: `databricks labs dqx open-remote-config`
# MAGIC
# MAGIC The config should look like this:
# MAGIC ```yaml
# MAGIC log_level: INFO
# MAGIC version: 1
# MAGIC run_configs:
# MAGIC - name: default
# MAGIC   input_location: /databricks-datasets/delta-sharing/samples/nyctaxi_2019
# MAGIC   input_format: delta
# MAGIC   output_table: skipped
# MAGIC   quarantine_table: main.nytaxi.quarantine
# MAGIC   checks_file: checks.yml
# MAGIC   profile_summary_stats_file: profile_summary_stats.yml
# MAGIC   warehouse_id: your-warehouse-id
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installation of DQX in the Databricks cluster
# MAGIC Once DQX is inslatted in the workspace, you need to install it in a cluster.

# COMMAND ----------

import glob
import os

user_name = spark.sql("select current_user() as user").collect()[0]["user"]
dqx_wheel_files = glob.glob(f"/Workspace/Users/{user_name}/.dqx/wheels/databricks_labs_dqx-*.whl")
dqx_latest_wheel = max(dqx_wheel_files, key=os.path.getctime)
%pip install {dqx_latest_wheel}
%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run profiler workflow to generate quality rule candidates
# MAGIC
# MAGIC The profiler generates and saves quality rule candidates (checks), offering an initial set of quality checks that can be customized and refined as needed.
# MAGIC
# MAGIC Run in your terminal: `databricks labs dqx profile --run-config "default"`
# MAGIC
# MAGIC You can also start the profiler by navigating to the Databricks Workflows UI.
# MAGIC
# MAGIC Note that using the profiler is optional. It is usually one-time operation and not a scheduled activity. The generated check candidates should be manually reviewed before being applied to the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run profiler from the code to generate quality rule candidates
# MAGIC
# MAGIC You can also run the profiler from the code directly, instead of using the profiler.

# COMMAND ----------

import yaml
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.utils import read_input_data
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
dq_engine = DQEngine(ws)
run_config = dq_engine.load_run_config(run_config_name="default", assume_user=True)

# read the input data, limit to 1000 rows for demo purpose
input_df = read_input_data(spark, run_config.input_location, run_config.input_format).limit(1000)

# profile the input data
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(input_df)
print(summary_stats)
print(profiles)

# generate DQX quality rules/checks
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)  # with default level "error"
print(yaml.safe_dump(checks))

# save generated checks to location specified in the default run configuration inside workspace installation folder
dq_engine.save_checks_in_installation(checks, run_config_name="default")
# or save it to an arbitrary workspace location
#dq_engine.save_checks_in_workspace_file(checks, workspace_path="/Shared/App1/checks.yml")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare checks manually and save in the workspace (optional)
# MAGIC
# MAGIC You can modify the check candidates generated by the profiler to suit your needs. Alternatively, you can create checks manually, as demonstrated below, without using the profiler.

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
- check:
    function: is_not_null
    arguments:
      col_names:
        - vendor_id
        - pickup_datetime
        - dropoff_datetime
        - passenger_count
        - trip_distance
        - pickup_longitude
        - pickup_latitude
        - dropoff_longitude
        - dropoff_latitude
  criticality: error
- check:
    function: is_not_less_than
    arguments:
      col_name: trip_distance
      limit: 1
  criticality: warn
- check:
    function: sql_expression
    arguments:
      expression: pickup_datetime <= dropoff_datetime
      msg: pickup time must not be greater than dropff time
      name: pickup_datetime_greater_than_dropoff_datetime
  criticality: error
- check:
    function: is_not_in_future
    arguments:
      col_name: pickup_datetime
  name: pickup_datetime_not_in_future
  criticality: warn
""")

# validate the checks
status = DQEngine.validate_checks(checks)
print(status)
assert not status.has_errors

dq_engine = DQEngine(WorkspaceClient())
# save checks to location specified in the default run configuration inside workspace installation folder
dq_engine.save_checks_in_installation(checks, run_config_name="default")
# or save it to an arbitrary workspace location
#dq_engine.save_checks_in_workspace_file(checks, workspace_path="/Shared/App1/checks.yml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply quality rules (checks) in the Lakehouse medallion architecture

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.utils import read_input_data
from databricks.sdk import WorkspaceClient

run_config = dq_engine.load_run_config(run_config_name="default", assume_user=True)

# read the data, limit to 1000 rows for demo purpose
bronze_df = read_input_data(spark, run_config.input_location, run_config.input_format).limit(1000)

# apply your business logic here
bronze_transformed_df = bronze_df.filter("vendor_id in (1, 2)")

dq_engine = DQEngine(WorkspaceClient())

# load checks from location defined in the run configuration
checks = dq_engine.load_checks_from_installation(assume_user=True, run_config_name="default")
# or load checks from arbitrary workspace file
# checks = dq_engine.load_checks_from_workspace_file(workspace_path="/Shared/App1/checks.yml")
print(checks)

# Option 1: apply quality rules and quarantine invalid records
silver_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(bronze_transformed_df, checks)
display(quarantine_df)

# Option 2: apply quality rules and flag invalid records as additional columns (`_warning` and `_error`)
#silver_valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(bronze_transformed_df, checks)
#display(silver_valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save quarantined data to Unity Catalog table
# MAGIC
# MAGIC Note: In this demo, we only save the quarantined data and omit the output. This is because the dashboard use only quarantined data as their input. Therefore, saving the output data is unnecessary in this demo. If you apply checks to flag invalid records without quarantining them (e.g. using the apply check methods without the split), ensure that the `quarantine_table` field in your run config is set to the same value as the `output_table` field.
# MAGIC

# COMMAND ----------

print(f"Saving quarantined data to {run_config.quarantine_table}")
quarantine_catalog, quarantine_schema, _ = run_config.quarantine_table.split(".")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {quarantine_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quarantine_catalog}.{quarantine_schema}")

quarantine_df.write.mode("overwrite").saveAsTable(run_config.quarantine_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### View data quality in DQX Dashboard

# COMMAND ----------

from databricks.labs.dqx.contexts.workspace import WorkspaceContext

ctx = WorkspaceContext(WorkspaceClient())
dashboards_folder_link = f"{ctx.installation.workspace_link('')}dashboards/"
print(f"Open a dashboard from the following folder and refresh it:")
print(dashboards_folder_link)