import os, sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), r"C:\VS Code\Customer-and-Sales-Data-Pipeline-using-AWS-PySpark-MySQL-main"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import datetime
import shutil
from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.move.move_files import move_s3_to_s3
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.read.aws_read import S3Reader
from src.main.download.aws_file_download import S3FileDownloader
from src.main.utility.spark_session import spark_session
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader

######Get AWS S3 Client and List Buckets######
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(aws_access_key, aws_secret_key)
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()

print(response)

logger.info("List of Buckets: %s", response['Buckets'])

#Check if local directory has already a file
#If file exists, check if the same file is present in the staging area with status as 'A'. If so, then don't delete and try to re-run.
#Else give an error and not process the next file.
if os.path.isdir(config.local_directory):
    csv_files = [file for file in os.listdir(config.local_directory) if file.endswith('.csv')]
else:
    print(f"Directory {config.local_directory} does not exist.")

connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []

if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f"""
    Select distinct file_name FROM
    {config.db_name}.{config.table_name}
    customer_sales_analysis.product_staging_table
    WHERE file_name IN ({str(total_csv_files)[1: -1]}) and status='I'
    """
    
    logger.info(f"Dynamically statement created: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run was failed please check the error logs.")
    else:
        logger.info("No files found in the staging area with status 'I'. You can proceed with the next file.")

else:
    logger.info("Last run was successful.")

try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                            config.bucket_name,
                                            folder_path=folder_path)
    logger.info("Absolute path available in the S3 bucket: %s", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info("No files found in {folder_path}")
        raise Exception("No Data available to process.")
    
except Exception as e:
    logger.error("Error occurred while listing files in S3 bucket: %s", e)
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logger.info("File path available on s3 under %s bucket and folder name is %s", bucket_name)
logger.info("File path available on s3 under {bucket_name} bucket and folder name is {file_paths}")

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File download error: %s", e)
    sys.exit()

#Get a list of all files present in the local directory after download
all_files = os.listdir(local_directory)
logger.info(f"List of files present at my local directory after download: {all_files}")

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith('.csv'):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))

    if not csv_files:
        logger.info("No CSV files found to process the request.")
        raise Exception("No CSV files found to process the request.")

else:
    logger.info("There is no data to process.")
    raise Exception("There is no data to process.")

logger.info("------------List of files that needs to be processed------------")
logger.info("List of csv files that needs to be processed %s", csv_files)

logger.info("------------Creating Spark Session------------")

spark = spark_session()

logger.info("------------Spark Session Created------------")

logger.info("------------Checking Schema for data loaded in s3------------")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
        .option("header", "true") \
        .load(data).columns
    logger.info(f"Schema of the file {data} is {data_schema}")
    logger.info(f"Mandatory columns are {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing columns in the file {data} are {missing_columns}")

    if missing_columns:
        logger.error(f"Mandatory columns are missing in the file {data}. Please check the file and try again.")
        error_files.append(data)
    else:
        logger.info(f"No missing columns in the file {data}. File is ready to process.")
        correct_files.append(data)

logger.info(f"------------List of Correct Files------------{correct_files}")
logger.info(f"------------List of Error Files------------{error_files}")
logger.info("------------Moving Error data to error directory if any------------")

error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        file_name = os.path.basename(file_path)
        destination_path = os.path.join(error_folder_local_path, file_name)
        
        shutil.move(file_path, destination_path)
        logger.info(f"Moved file {file_path} to error directory {destination_path}")

        source_prefix = config.s3_source_directory
        destination_prefix = config.s3_error_directory

        message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
        logger.info(f"{message}")
        
    else:
        logger.error(f"'{file_path}' doesn't exist.")
else:
    logger.info("------------There are no error files available in our dataset------------")

logger.info("------------Updating the product staging table that we have started the process------------")
insert_statement = []
db_name = config.database_name
current_date = datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        statements = f"INSERT INTO {db_name}.{config.product_staging_table}" \
                     f"(file_name, file_location, created_date, status)" \
                     f" VALUES ('{file_name}', '{file_name}', '{formatted_date}', 'I')"
        
        insert_statement.append(statements)
    logger.info(f"Insert statement created for staging table: {insert_statement}")
    logger.info("------------Connecting with MySQL server------------")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("------------Connected with MySQL server------------")
    for statement in insert_statement:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("There are no files to process. Please check the error logs.")
    raise Exception("There is no data available with correct files")

logger.info("------------Staging table updates successfully------------")

logger.info("------------Fixing extra column coming from source------------")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True)
    StructField("additional_column", StringType(), True)
])

database_client = DatabaseReader(config.url, config.properties)
logger.info("------------Creating empty dataframe------------")
final_df_to_process = spark.createDataFrame([], schema=schema)
final_df_to_process.show()

for data in correct_files:
    data_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(schema.fieldNames()))
    logger.info(f"Extra columns present at source is {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_column", concat_ws(",", *extra_columns)) \
            .select("customer_id", "store_id", "product_name", "sales_date",
                    "sales_person_id", "price", "quantity", "total_cost", "additional_column")
        
        logger.info(f"processed {data} and added 'addional_column'")
    else:
        data_df = data_df.withColumn("additional_column", lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date",
                    "sales_person_id", "price", "quantity", "total_cost", "additional_column")
        
    final_df_to_process = final_df_to_process.union(data_df)

logger.info("------------Final Dataframe from source which will be going to processing------------")
final_df_to_process.show()

database_client = DatabaseReader(config.url, config.properties)

#Customer Table
logger.info("------------Loading Customer Table into customer_table_df------------")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)

#Product Table
logger.info("------------Loading Product Table into product_table_df------------")
product_table_df = database_client.create_dataframe(spark, config.product_table)

#product_staging_table Table
logger.info("------------Loading Product Staging Table into product_staging_table_df------------")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table_name)

#sales_team Table
logger.info("------------Loading Sales Team Table into sales_team_table------------")
sales_team_df = database_client.create_dataframe(spark, config.sales_team_table_name)

logger.info("------------Calculating customer every month purchase amount------------")
customer_mart_calculation_table_write(final_customer_data_mart_df)

logger.info("------------Calculation of customer mart done and written into the table------------")

logger.info("------------Calculating customer every month billed amount------------")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)

logger.info("------------Calculation of sales mart done and written into the table------------")

source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info("------------Deleting sales data from local------------")
delete_local_file(config.local_directory)
logger.info("------------Deleted sales data from local------------")

logger.info("------------Deleting Sales from Local------------")
delete_local_file(config.customer_data_mart_local_file)
logger.info("------------Deleted sales data from local------------")

logger.info("------------Deleting Sales from Local------------")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("------------Deleted sales data from local------------")

logger.info("------------Deleting Sales from Local------------")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("------------Deleted sales data from local------------")

update_statements = []
if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        update_statement = f"UPDATE {db_name}.{config.product_staging_table} " \
                           f"SET status='I', updated_date='{formatted_date}' " \
                           f"WHERE file_name='{file_name}'"
        
        update_statements.append(update_statement)

    logger.info(f"Update statement created for staging table: -- {update_statements}")
    logger.info("------------Connecting with MySQL server------------")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("------------Connected with MySQL server------------")

    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("There are no files to process. Please check the error logs.")
    raise Exception("There is no data available with correct files")


input("Press Enter to exit...")


