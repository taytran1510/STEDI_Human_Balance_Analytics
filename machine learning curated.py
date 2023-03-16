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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="projectlakehouse",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="projectlakehouse",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Join Customer and Step Trainer
JoinCustomerandStepTrainer_node1 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainerTrusted_node1,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="JoinCustomerandStepTrainer_node1",
)

# Script generated for node Drop Fields
DropFields_node1 = DropFields.apply(
    frame=JoinCustomerandStepTrainer_node1,
    paths=["user"],
    transformation_ctx="DropFields_node1",
)

# Script generated for node Machine Learning Curated
machine_learning_curated_node1 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1,
    database="projectlakehouse",
    table_name="machine_learning_curated",
    transformation_ctx="machine_learning_curated_node1",
)


job.commit()