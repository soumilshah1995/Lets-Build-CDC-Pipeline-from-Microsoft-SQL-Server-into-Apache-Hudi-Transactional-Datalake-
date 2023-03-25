import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://sql-server-dms-demo/raw/dbo/invoice/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("Op", "string", "Op", "string"),
        ("invoiceid", "string", "invoiceid", "string"),
        ("itemid", "string", "itemid", "string"),
        ("category", "string", "category", "string"),
        ("price", "string", "price", "string"),
        ("quantity", "string", "quantity", "string"),
        ("orderdate", "string", "orderdate", "string"),
        ("destinationstate", "string", "destinationstate", "string"),
        ("shippingtype", "string", "shippingtype", "string"),
        ("referral", "string", "referral", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
additional_options = {
    "hoodie.table.name": "invoice",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "invoiceid",
    "hoodie.datasource.write.precombine.field": "Op",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.parquet.compression.codec": "gzip",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "edw",
    "hoodie.datasource.hive_sync.table": "tbl_invoice",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
}
S3bucket_node3_df = ApplyMapping_node2.toDF()
S3bucket_node3_df.write.format("hudi").options(**additional_options).mode(
    "append"
).save("s3://sql-server-dms-demo/hudi/dbo/invoice/")

job.commit()
