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

# Script generated for node CustomerTrusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="projectlakehouse",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="projectlakehouse",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometer_Landing_node1",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=Accelerometer_Landing_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1 = DropFields.apply(
    frame=Join_node2,
    paths=["x", "y", "z", "user", "timestamp"],
    transformation_ctx="DropFields_node1",
)

# Script generated for node Customer Curated
customer_curated_node1 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1,
    database="projectlakehouse",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1",
)

job.commit()