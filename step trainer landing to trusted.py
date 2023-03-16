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

# Script generated for node customercurated
customercurated_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="projectlakehouse",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1",
)

# Script generated for node steptrainerlanding
steptrainerlanding_node1678886253518 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://thuantn5-datalake/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainerlanding_node1678886253518",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=customercurated_node1,
    frame2=steptrainerlanding_node1678886253518,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1678886479487 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "serialnumber",
    ],
    transformation_ctx="DropFields_node1678886479487",
)

# Script generated for node steptrainertrusted
steptrainertrusted_node1678886607751 = glueContext.getSink(
    path="s3://thuantn5-datalake/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="steptrainertrusted_node1678886607751",
)
steptrainertrusted_node1678886607751.setCatalogInfo(
    catalogDatabase="projectlakehouse", catalogTableName="step_trainer_trusted"
)
steptrainertrusted_node1678886607751.setFormat("json")
steptrainertrusted_node1678886607751.writeFrame(DropFields_node1678886479487)
job.commit()
