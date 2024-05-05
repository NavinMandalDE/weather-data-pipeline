import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

current_date = datetime.datetime.now().strftime("%Y-%m-%d")
bucket_name = 'pf-weather-data'
# file_name =  'weather_api_data.csv'
data_path = f"s3://{bucket_name}/date={current_date}/"
redshift_role_arn = "arn:aws:iam::654654491149:role/redshift-role"
redshift_tmp_dir = "s3://s3-temp-misc/weather-temp/"

# Script generated for node Amazon S3
AmazonS3_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, 
    connection_type="s3", 
    format="csv", 
    connection_options={"paths": [data_path], 
                        "recurse": True}, 
    transformation_ctx="AmazonS3_node"
    )

# Script generated for node Change Schema
ChangeSchema_node = ApplyMapping.apply(frame=AmazonS3_node, 
                                                    mappings=[
                                                        ("dt", "string", "dt", "string"),
                                                        ("weather", "string", "weather", "string"),
                                                        ("visibility", "string", "visibility", "string"),
                                                        ("`main.temp`", "string", "temp", "string"),
                                                        ("`main.feels_like`", "string", "feels_like", "string"),
                                                        ("`main.temp_min`", "string", "min_temp", "string"),
                                                        ("`main.temp_max`", "string", "max_temp", "string"),
                                                        ("`main.pressure`", "string", "pressure", "string"),
                                                        ("`main.sea_level`", "string", "sea_level", "string"),
                                                        ("`main.grnd_level`", "string", "ground_level", "string"),
                                                        ("`main.humidity`", "string", "humidity", "string"),
                                                        ("`wind.speed`", "string", "wind", "string")
                                                    ], 
                                                    transformation_ctx="ChangeSchema_node"
                                            )

Redshift_node =  glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": redshift_tmp_dir,
        "useConnectionProperties": "true",
        "aws_iam_role": redshift_role_arn,
        "dbtable": "openweather.weather_data",
        "connectionName": "redshift-connection",
        "preactions": "DROP TABLE IF EXISTS openweather.weather_data; CREATE TABLE IF NOT EXISTS openweather.weather_data (dt VARCHAR, weather VARCHAR, visibility VARCHAR, temp VARCHAR, feels_like VARCHAR, min_temp VARCHAR, max_temp VARCHAR, pressure VARCHAR, sea_level VARCHAR, ground_level VARCHAR, humidity VARCHAR, wind VARCHAR);",
    },
    transformation_ctx="Redshift_node",
)

job.commit()
