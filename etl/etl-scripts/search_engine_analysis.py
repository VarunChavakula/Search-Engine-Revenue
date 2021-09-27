"""
This Glue job reads Archibus Room data from JDBC connection to on-premise database,
hashes the Personally Identifiable Information and writes it to the Archibus RM table.
"""

import sys
from datetime import date
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import sum
from io import StringIO
import boto3


try:
    ARGS = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'inputfile'])
    INPUT = ARGS['inputfile']
    s3_resource = boto3.resource('s3')
except KeyError as key_error:
    print("Required environment variables are not set",key_error)
    sys.exit()

BUCKET = "s3://aws-glue-search-engine-revenue/"
OUTPUT = "_SearchKeywordPerformance"
spark = SparkSession.builder.appName("Python Spark SQL basic example")\
    .config("spark.some.config.option", "some-value").getOrCreate()

def main():
    """
    This function reads data from s3, hashes and performs few transformations on it.
    """
    df_source = spark.read.csv(BUCKET + INPUT, sep=r'\t', header=True)
    df_source.createOrReplaceTempView("hits")
    transformation()
    
def transformation():
    df_product_searched = spark.sql("(select * from (select rank() over (PARTITION BY ip \
        ORDER BY hit_time_gmt asc) as rank, ip, referrer from hits) where rank =1)")
    df_product_bought = spark.sql("select ip,product_list from hits \
        where pagename='Order Complete'")
    df_product_searched_bought  = df_product_searched.join\
    (df_product_bought,df_product_searched.ip == df_product_bought.ip,"left")
    df_product_searched_bought = df_product_searched_bought.select("product_list","referrer")
    df_product_searched_bought.createOrReplaceTempView('search_engine_revenue')
    df_search_engine_extracted = spark.sql("select split(product_list,';')[2] as quantity,\
        split(product_list,';')[3] as revenue,split(referrer,'/')[2] as search_engine,\
        split(referrer,'/')[3] as query_params from search_engine_revenue")
    df_search_engine_extracted.createOrReplaceTempView('search_engine_revenue')

    df_search_key_extracted = spark.sql("select concat(lower(split(search_engine,'[.]')[1]),'.com')\
        as `Search Engine Domain`, split(query_params,'[&?]')[1]\
        as search_key_yahoo,split(query_params,'[&?]')[1] as search_key_bing,\
        split(query_params,'[&?]')[5] as search_key_google,revenue\
         as `Revenue` from search_engine_revenue")

    df_search_key_extracted.createOrReplaceTempView('search_engine_revenue')
    df_search_engine_revenue = spark.sql("select `Search Engine Domain`,\
         lower(substring(search_key_yahoo,3))\
        as `Search Keyword` , ifnull(Revenue,0) \
        as Revenue from search_engine_revenue where search_key_yahoo like 'p=%' \
        union select `Search Engine Domain`, lower(substring(search_key_bing,3)) \
        as `Search Keyword`, ifnull(Revenue,0) \
        as Revenue from search_engine_revenue where search_key_bing like 'q=%' \
        union select `Search Engine Domain`, lower(substring(search_key_google,3)) \
        as `Search Keyword`, ifnull(Revenue,0)\
        as Revenue from search_engine_revenue where search_key_google like 'q=%'")

    df_search_engine_revenue = df_search_engine_revenue\
        .withColumn("Revenue",df_search_engine_revenue.Revenue.cast(IntegerType()))
    df_search_engine_revenue = df_search_engine_revenue\
        .groupby('Search Engine Domain', 'Search Keyword').agg(sum("Revenue").alias("Revenue"))
    df_search_engine_revenue = df_search_engine_revenue\
        .sort(df_search_engine_revenue.Revenue.desc())
    df_search_engine_revenue.show()
    df_search_engine_revenue.write\
        .csv(BUCKET + str(date.today()) + OUTPUT, sep=r'\t', mode = "overwrite" )
    pandas_df_search_engine_revenue = df_search_engine_revenue.toPandas()
    csv_buffer = StringIO()
    pandas_df_search_engine_revenue.to_csv(csv_buffer, index=False)
    s3_resource.Object('aws-glue-search-engine-revenue',\
         str(date.today()) + OUTPUT +'.tsv').put(Body=csv_buffer.getvalue())

if __name__ == '__main__':
    main()
    