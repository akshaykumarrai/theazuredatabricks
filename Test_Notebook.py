# Databricks notebook source
dbutils.secrets.list("databricks_secret_scope")

# COMMAND ----------

storage_account_name = "adls27"
client_id = dbutils.secrets.get(scope="databricks_secret_scope",key="databricks-app-client-id")
tenant_id = dbutils.secrets.get(scope="databricks_secret_scope",key="databricks-app-tenant-id")
client_secret = dbutils.secrets.get(scope="databricks_secret_scope",key="databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret":f"{client_secret}" ,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
def mount_adls(container):
    dbutils.fs.mount(
      source = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container}",
      extra_configs = configs)

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/adls27/raw")

# COMMAND ----------


