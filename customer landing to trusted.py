import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customerlanding
customerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://thuantn5-datalake/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customerlanding_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Filter.apply(
    frame=customerlanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node customertrusted
customertrusted_node1678881184529 = glueContext.getSink(
    path="s3://thuantn5-datalake/customer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customertrusted_node1678881184529",
)
customertrusted_node1678881184529.setCatalogInfo(
    catalogDatabase="projectlakehouse", catalogTableName="customer_trusted"
)
customertrusted_node1678881184529.setFormat("json")
customertrusted_node1678881184529.writeFrame(ApplyMapping_node2)
job.commit()
