def mount_blob_storage(storage_account_name, container_name, mount_point, access_key):
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
        mount_point=mount_point,
        extra_configs={
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key
        }
    )




dbutils.widgets.text("storage_account_name", "storageforsurya", "Storage Account Name")
dbutils.widgets.text("container_name", "silver", "Container Name")
dbutils.widgets.text("mount_point", "/mnt/silver", "Mount Point")
dbutils.widgets.text("access_key", "2f8TlZe0H3dmCD2h6Rq/ZKPsGieaRHlIgCScj2MdjcOCBpap20fLEII5MU2amvsyu0GcDNLa3L6V+ASt0368OA==", "Access Key")



storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
mount_point = dbutils.widgets.get("mount_point")
access_key = dbutils.widgets.get("access_key")



mount_blob_storage(storage_account_name, container_name, mount_point, access_key)

# code synatax



from pyspark.sql.functions import udf
def toSnakeCase(df):
    for column in df.columns:
        snake_case_col = ''
        for char in column:
            if char ==' ':
                snake_case_col += '_'
            else:
                snake_case_col += char.lower()
        df = df.withColumnRenamed(column, snake_case_col)
    return df

udf(toSnakeCase)



def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)



def read_delta_file(delta_path):
    df = spark.read.format("delta").load(delta_path)
    return df
udf(read_delta_file)




