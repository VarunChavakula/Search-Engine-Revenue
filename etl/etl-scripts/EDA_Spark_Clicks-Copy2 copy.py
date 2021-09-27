import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions


try:
    ARGS = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'inputfile'])
    INPUT = ARGS['inputfile']
except KeyError as key_error:
    print("Required environment variables are not set",key_error)
    sys.exit()

def main():
    print(INPUT)
    spark = SparkSession.builder.appName("Python Spark SQL basic example")\
        .config("spark.some.config.option", "some-value").getOrCreate()
    df = spark.read.csv('s3://aws-glue-search-engine-revenue/'+INPUT, sep=r'\t', header=True)
    df.createOrReplaceTempView("hits")
    df_product_searched = spark.sql("(select * from (select rank() over (PARTITION BY ip \
        ORDER BY hit_time_gmt asc) as rank, ip, referrer from hits) where rank =1)")
    df_product_bought = spark.sql("select ip,product_list from hits where pagename='Order Complete'")
    df_product_searched_bought  = df_product_searched.join(df_product_bought,df_product_searched.ip== df_product_bought.ip,"left")
    df_product_searched_bought= df_product_searched_bought.select("product_list","referrer")
    df_product_searched_bought.createOrReplaceTempView('search_engine_revenue')
    df_search_engine_extracted = spark.sql("select split(product_list,';')[2] as quantity,\
        split(product_list,';')[3] as revenue,split(referrer,'/')[2] as search_engine,\
        split(referrer,'/')[3] as query_params from search_engine_revenue")
    df_search_engine_extracted.createOrReplaceTempView('search_engine_revenue')

    df_search_key_extracted = spark.sql("select concat(lower(split(search_engine,'[.]')[1]),'.com')  as `Search Engine Domain`,\
                    split(query_params,'[&?]')[1] as search_key_yahoo,split(query_params,'[&?]')[1] as search_key_bing,\
                    split(query_params,'[&?]')[5] as search_key_google,revenue as `Revenue` from search_engine_revenue")

    df_search_key_extracted.createOrReplaceTempView('search_engine_revenue')
    df_search_engine_revenue = spark.sql("select `Search Engine Domain`, lower(substring(search_key_yahoo,3))\
        as `Search Keyword` , ifnull(Revenue,0) as Revenue from search_engine_revenue where search_key_yahoo like 'p=%' \
        union select `Search Engine Domain`, lower(substring(search_key_bing,3)) as `Search Keyword`, ifnull(Revenue,0) \
        as Revenue from search_engine_revenue where search_key_bing like 'q=%' \
        union select `Search Engine Domain`, lower(substring(search_key_google,3)) as `Search Keyword`, ifnull(Revenue,0)\
        as Revenue from search_engine_revenue where search_key_google like 'q=%'")

    df_search_engine_revenue = df_search_engine_revenue.withColumn("Revenue",col("Revenue").cast(IntegerType()))
    df_search_engine_revenue = df_search_engine_revenue.groupby('Search Engine Domain', 'Search Keyword')\
        .agg(sum("Revenue").alias("Revenue")).orderBy(col("Revenue").desc())
    df_search_engine_revenue.show()

if __name__ == '__main__':
    main()