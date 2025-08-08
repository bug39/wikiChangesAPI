from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder\
    .appName("wikiChanges")\
        .getOrCreate()
        
        
#how to automate this?
raw_df = spark.read.json("/Users/mish/Documents/Code/Projects/wikiEmbeddings/data/20000.json")


clean_df = raw_df.select(
    col("meta.dt").alias("timestamp"),
    "user",
    "type",
    "title",
    "bot",
    "wiki",
    "comment",
    col("meta.uri").alias("uri"),
    col("meta.domain").alias("domain")
)
clean_df.head(5)
# print(clean_df.head(5))
# clean_df.write \
#     .mode("overwrite") \
#     .partitionBy("timestamp") \
#     .parquet("s3://wikichangesbucket/processed/")
