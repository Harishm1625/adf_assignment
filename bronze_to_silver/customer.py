

from pyspark.sql.functions import split, when, col, to_date



raw_customer_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/customer/20240107_sales_customer.csv', header=True, inferSchema=True)



renamed_customer_df = toSnakeCase(raw_customer_df)



splited_name_df = renamed_customer_df.withColumn('first_name', split(renamed_customer_df.name, " ")[0])\
    .withColumn('last_name', split(renamed_customer_df.name, " ")[1]).drop(renamed_customer_df.name)



extract_domain_df = splited_name_df.withColumn("tempdomain", split(splited_name_df.email_id, "@")[1]).drop(splited_name_df.email_id)
extract_domain_df = extract_domain_df.withColumn('domain', split(extract_domain_df.tempdomain, '\.')[0]).drop(extract_domain_df.tempdomain)



converted_gender_df = extract_domain_df.withColumn('gender', when(col('gender') == 'male', 'M')\
    .otherwise('F'))



splited_join_date_df = converted_gender_df.withColumn('date', split(col('joining_date'), " ")[0])\
    .withColumn('time', split(col('joining_date'), ' ')[1]).drop('joining_date')




converted_date_df = splited_join_date_df.withColumn('date', to_date(col('date'), 'dd-MM-yyyy'))



expenditure_df = converted_date_df.withColumn('expenditure-status', when(col('spent') < 200, 'MINIMUM').otherwise('MAXIMUM'))



writeTo = f'dbfs:/mnt/silver/sales_view/customer'
expenditure_df.write.format('delta').save(writeTo)
