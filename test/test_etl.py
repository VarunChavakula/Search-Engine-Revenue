
from et.etl-scripts.search_engine_analysis import transformation
from nose.tools import assert_equal
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Python Spark SQL basic example")\
    .config("spark.some.config.option", "some-value").getOrCreate()
df_source = spark.sql("select distinct(split(referrer,'/')[2]) from hits where referrer not like '%esshopzilla%'")

class AnagramTest(object):
    
    def test(self,sol):
        assert_equal(sol().count(),df_source.count())

        print("Test case passed")

t = AnagramTest()
t.test(transformation)
