from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder\
    .appName("wikiChanges")\
        .getOrCreate()
        
        
#todo automate this
raw_df = spark.read.json("/Users/mish/Documents/Code/Projects/wikiEmbeddings/data/20000.json")

#python below
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
).filter(col("bot") == False)


#wait on this
# clean_df.write \
#     .mode("overwrite") \
#     .partitionBy("timestamp") \
#     .parquet("s3://wikichangesbucket/processed/")







# #sql
# cleanql = spark.sql(
#     """
#     SELECT 
#     CAST(meta.dt AS TIMESTAMP) AS timestamp,
#     user,
#     type,
#     comment,
#     wiki,
#     meta.uri AS uri,
#     meta.domain AS domain
#     FROM raw_df
#     WHERE bot == false                
                    
                    
#     """    
# )


