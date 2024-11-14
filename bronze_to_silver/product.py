



from pyspark.sql.functions import when, col



raw_producet_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/product/20240107_sales_product.csv', header=True, inferSchema=True)



renamed_product_df = toSnakeCase(raw_producet_df)



sub_category_df = renamed_product_df.withColumn("sub_category", when(col('category_id') == 1, "phone")\
        .when(col('category_id') == 2 , "laptop")\
        .when(col('category_id') == 3, "playstation")\
        .when(col('category_id') == 4, "e-device"))




writeTo = f'dbfs:/mnt/silver/sales_view/product'
write_delta_upsert(sub_category_df, writeTo)



