# LIBRARY IMPORTS

# Import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

# This makes scheduling easy
from airflow.utils.dates import days_ago
import requests

# # Define the path for the input and output files
# input_file = 'web-server-access-log.txt'
# extracted_file = 'extracted-data.txt'
# transformed_file = 'transformed.txt'
# output_file = 'capitalized.txt'


# def download_file():
#     url= "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
#     # Send a GET request to the URL
#     with requests.get(url, stream=True) as response:
#         # Raise an exception for HTTP errors
#         response.raise_for_status()
#         # Open a local file in binary write mode
#         with open(input_file, 'wb') as file:
#             # Write the content to the local file in chunks
#             for chunk in response.iter_content(chunk_size=8192):
#                 file.write(chunk)
#     print(f"File downloaded successfully: {input_file}")


# def extract():
#     global input_file
#     print("Inside Extract")
#     # Read the contents of the file into a string
#     with open(input_file, 'r') as infile, \
#             open(extracted_file, 'w') as outfile:
#         for line in infile:
#             fields = line.split('#')
#             if len(fields) >= 4:
#                 field_1 = fields[0]
#                 field_4 = fields[3]
#                 outfile.write(field_1 + "#" + field_4 + "\n")


# def transform():
#     global extracted_file, transformed_file
#     print("Inside Transform")
#     with open(extracted_file, 'r') as infile, \
#             open(transformed_file, 'w') as outfile:
#         for line in infile:
#             processed_line = line.upper()
#             outfile.write(processed_line + '\n')


# def load():
#     global transformed_file, output_file
#     print("Inside Load")
#     # Save the array to a CSV file
#     with open(transformed_file, 'r') as infile, \
#             open(output_file, 'w') as outfile:
#         for line in infile:
#             outfile.write(line + '\n')


# def check():
#     global output_file
#     print("Inside Check")
#     # Save the array to a CSV file
#     with open(output_file, 'r') as infile:
#         for line in infile:
#             print(line)

# DAG ARGUMENTS

# Define default arguments for the DAG
default_args = {
    'owner': 'bilon',
    'start_date': days_ago(0),
    'email': ['bilon@email.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG DEFINITION

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
)

# TASKS DEFINITIONS

# Define the task named unzip_data to unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzvf /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/tolldata.tgz -C /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/',
    dag=dag,
)
# # Define the task named unzip_data to unzip data
# unzip_data = BashOperator(
#     task_id='unzip_data',
#     bash_command='tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
#     dag=dag,
# )

# Define the task named extract_data_from_csv to extract data
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="""
    awk -F ',\t' 'BEGIN {OFS=","} {if(NR>1) print $1,$2,$3,$4}' /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/vehicle-data.csv > /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/csv_data.csv
    """,
    dag=dag,
)
# # Define the task named extract_data_from_csv to extract data
# extract_data_from_csv = BashOperator(
#     task_id='extract_data_from_csv',
#     bash_command="""
#     awk -F ',\t' 'BEGIN {OFS=","} {if(NR>1) print $1,$2,$3,$4}' /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv
#     """,
#     dag=dag,
# )

# Define the task named extract_data_from_tsv to extract data
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="""
    awk -F '\\t' 'BEGIN {OFS=","} {if(NR>1) print $5,$6,$7}' /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/tollplaza-data.tsv > /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/tsv_data.csv
    """,
    dag=dag,
)
# # Define the task named extract_data_from_tsv to extract data
# extract_data_from_tsv = BashOperator(
#     task_id='extract_data_from_tsv',
#     bash_command="""
#     awk -F '\\t' 'BEGIN {OFS=","} {if(NR>1) print $5,$6,$7}' /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv
#     """,
#     dag=dag,
# )

# Define the task named extract_data_from_fixed_width to extract data
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
    awk '{print substr($0, 62, 3) "," substr($0, 66)}' /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/payment-data.txt > /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/fixed_width_data.csv
    """,
    dag=dag,
)
# # Define the task named extract_data_from_fixed_width to extract data
# extract_data_from_fixed_width = BashOperator(
#     task_id='extract_data_from_fixed_width',
#     bash_command="""
#     awk '{print substr($0, 62, 3) "," substr($0, 66)}' /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv
#     """,
#     dag=dag,
# )

# Define the task named consolidate_data to consolidate data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
    paste -d ',' /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/csv_data.csv /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/tsv_data.csv /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/fixed_width_data.csv > /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/extracted_data.csv
    """,
    dag=dag,
)
# # Define the task named consolidate_data to consolidate data
# consolidate_data = BashOperator(
#     task_id='consolidate_data',
#     bash_command="""
#     paste -d ',' /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv
#     """,
#     dag=dag,
# )

# Define the task named transform_data to transform data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    awk -F',' 'BEGIN {OFS=","} {if(NR==1) print $0; else {$4=toupper($4); print $0}}' /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/extracted_data.csv > /mnt/d/Users/RJ/Career_Growth/IBM Data Engineering/8. ETL and Data Pipelines with Shell, Airflow and Kafka/M5 - Final Assignment/airflowbash-proj/airflow/dags/finalassignment/transformed_data.csv
    """,
    dag=dag,
)

# Define the task named transform_data to transform data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    awk -F',' 'BEGIN {OFS=","} {if(NR==1) print $0; else {$4=toupper($4); print $0}}' /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv
    """,
    dag=dag,
)

# TASK PIPELINE

# Set task dependencies
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

