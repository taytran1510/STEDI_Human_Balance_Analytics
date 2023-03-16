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

# Script generated for node customertrusted
customertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://thuantn5-datalake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1",
)

# Script generated for node accelerometerlanding
accelerometerlanding_node1678883029364 = glueContext.create_dynamic_frame.from_catalog(
    database="projectlakehouse",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometerlanding_node1678883029364",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=customertrusted_node1,
    frame2=accelerometerlanding_node1678883029364,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1678882960953 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "shareWithFriendsAsOfDate",
        "phone",
        "lastUpdateDate",
        "email",
        "customerName",
    ],
    transformation_ctx="DropFields_node1678882960953",
)

# Script generated for node accelerometertrusted
accelerometertrusted_node1678883134183 = glueContext.getSink(
    path="s3://thuantn5-datalake/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometertrusted_node1678883134183",
)
accelerometertrusted_node1678883134183.setCatalogInfo(
    catalogDatabase="projectlakehouse", catalogTableName="accelerometer_trusted"
)
accelerometertrusted_node1678883134183.setFormat("json")
accelerometertrusted_node1678883134183.writeFrame(DropFields_node1678882960953)
job.commit()
