# Databricks notebook source
import mlflow
mlflow.autolog(disable=True)
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

run_ids = mlflow.search_runs(experiment_ids=['d971c4ca577f44cfaae3afb42aa1bc13'])
run_ids


# COMMAND ----------

run_id = run_ids.iloc[0].run_id

# COMMAND ----------

run_id

# COMMAND ----------

model_version = mlflow.register_model(f"runs:/{run_id}/MAPIE-LGBM-regress-tweedie", "portfolio.end_to_end_demand_forecast.MAPIE-LGBM-regress-tweedie_2w1")


# COMMAND ----------

model_version.version

# COMMAND ----------

# add description of registered model
client = mlflow.tracking.MlflowClient()
client.update_registered_model(name="portfolio.end_to_end_demand_forecast.MAPIE-LGBM-regress-tweedie_2w1", description="""**demand forecast for fashion**

- **features** grends lag 1 and 2, customer statistics lag 1 and 2
- **output** input 2 weeks sales records and forecast confidence interval (alpha=0.15) of 1 week ahead
- **approved** Takehiro Ohashi
""")

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
client.set_registered_model_alias(
  name="portfolio.end_to_end_demand_forecast.MAPIE-LGBM-regress-tweedie_2w1",
  version=model_version.version,
  alias="Production",  # None, Staging, Production, Archived
)

# COMMAND ----------

# MAGIC %md
# MAGIC None: The default stage when a model version is first registered. It has no special designation and is usually not ready for use in any environment.
# MAGIC
# MAGIC Staging: This stage is used for models that are in the testing or validation phase. Models in this stage are undergoing additional evaluation to ensure they perform as expected. Use this stage to perform integration tests, A/B tests, and further validation before moving to production.
# MAGIC
# MAGIC Production: Models in this stage are considered production-ready and are actively used for making predictions in a live environment. This is the most critical stage as it impacts end-users or business decisions. Only models that have passed rigorous testing and validation should be promoted to this stage.
# MAGIC
# MAGIC Archived: Models that are no longer actively used but need to be retained for historical or audit purposes are moved to this stage. Archiving helps to keep the registry clean and ensures that only relevant models are visible in active stages.
