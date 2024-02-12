# Required libraries and modules
import os
import shutil
import requests
import json
import datetime
import torch
from pyspark.sql import SparkSession
import pandas as pd
from torch import nn
from kafka import KafkaConsumer, TopicPartition
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType ,FloatType
from pyspark.sql import types as T
from pyspark.sql.functions import col, from_json,pandas_udf, PandasUDFType

# ... (importing various functions from the graph2_S module)
from graph2_S import combined_polling_data,combined_regionwilayat_polling, pollingstats,genderstats,agewisePolling_stats,genderwise_Prediction, agewise_Prediction, regionwise_gender_stats, wilayatwise_gender_stats,\
            Ageinterval_stats,Registered_year_wise_stats,Regionwise_stats,regionwiseage_stats,wilayatwiseage_stats,regionwilayat_pollingstats,regionwilayat_genderstats ,regionwilayatgender_prediction,regionwilayatageInterval_stats

import torch.nn.functional as f
import pyodbc
from kafka import KafkaConsumer
import time
from sklearn.preprocessing import MinMaxScaler

# Warnings setup to ignore specific types of warnings
import warnings
warnings.filterwarnings("ignore")
warnings.filterwarnings("ignore", message="A NumPy version >=1.16.5 and <1.23.0 is required for this version of SciPy")
warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")
import pickle
import os

# Setting up environment variable
os.environ['NUMEXPR_MAX_THREADS'] = '68'

# Database connection string details
server = '127.0.0.1,1433' 
database = 'MOI_AI' 
username = 'sa' 
password = 'Password!' 

# Creating the connection string
conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=' + server + ';'
    r'DATABASE=' + database + ';'
    r'UID=' + username + ';'
    r'PWD=' + password
)

# Scaling setup
minmaxscaler = MinMaxScaler()

# Database connection setup
global conn 
conn = pyodbc.connect(conn_str)

print("""> connected to summary table""")

# Output schema for the UDF
output_schema = StructType([StructField("RegionID", StringType(), True),StructField("WilayatID", StringType(), True),StructField("Male", FloatType(), True),
                StructField("Female", FloatType(), True),StructField("col_1", FloatType(), True),StructField("col_2", FloatType(), True),StructField("col_3", FloatType(), True),
                StructField("col_4", FloatType(), True),StructField("col_5", FloatType(), True),StructField("col_6", FloatType(), True),StructField("col_7", FloatType(), True)])

@pandas_udf(output_schema, PandasUDFType.GROUPED_MAP)
def process_group_udf(pdf):
    """
    A UDF to process the grouped dataframe.
    Args:
    - pdf: Pandas DataFrame representing the group

    Returns:
    - Pandas DataFrame after processing
    """
    region_id = pdf["RegionID"].iloc[0]
    wilayat_id = pdf["WilayatID"].iloc[0]
    tensor_data = prepare_tensor_data(pdf,wilayat_id)
    prediction = obtain_model_prediction(models, wilayat_id, tensor_data).cumsum(axis=0)
    result = pd.DataFrame({"RegionID": [str(region_id)],"WilayatID": [str(wilayat_id)],
                            "Male": prediction[0][0],"Female": prediction[0][1], "col_1": prediction[0][2],
                            "col_2": prediction[0][3],"col_3": prediction[0][4],"col_4": prediction[0][5],
                            "col_5": prediction[0][6],"col_6": prediction[0][7],"col_7": prediction[0][8]}, 
                        index=[0])  
    return result

def prepare_tensor_data(pdf, wilayat_id):
    """
    Prepare the tensor data from the given dataframe.
    Args:
    - pdf: Input Pandas DataFrame
    - wilayat_id: Identifier for the wilayat

    Returns:
    - Tensor data ready for prediction
    """

    columns_to_include = ['Male', 'Female', 'col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6','col_7']
    rename_mapping = {'Male': 'Male','Female': 'Female','col_1': '20-30','col_2': '30-40','col_3': '40-50',
                      'col_4': '50-60','col_5': '60-70','col_6': '70-80','col_7': '80+'}
    selected_df = pdf[columns_to_include].head(60)
    missing_rows = 60 - selected_df.shape[0]

    if missing_rows > 0:
        padding_df = pd.DataFrame(np.zeros((missing_rows, len(columns_to_include))), columns=columns_to_include)
        selected_df = pd.concat([selected_df, padding_df], axis=0)
    
    selected_df.rename(columns=rename_mapping, inplace=True)
    scaler_key = f"scaler_{wilayat_id}"
    if scaler_key in scalers_dict:
        scaler = scalers_dict[scaler_key]
        selected_df_transformed = scaler.transform(selected_df)
    else:
        selected_df_transformed = minmaxscaler.fit_transform(selected_df)
    tensor_data = torch.tensor(selected_df_transformed, dtype=torch.float32)
    tensor_data = tensor_data.view(1, 60, len(columns_to_include))
    return tensor_data

def obtain_model_prediction(models, model_key, tensor_data):

    """
    Obtain the model prediction using the provided tensor data.
    Args:
    - models: Dictionary containing all the trained models
    - model_key: Key to access the appropriate model for prediction
    - tensor_data: Input tensor for prediction

    Returns:
    - Prediction as numpy array
    """
 
    if tensor_data is not None:
        
        model = models[f'model_{model_key}']
        model.eval()
        device = "cpu"
        tensor_data = tensor_data.float().to(device)
        output = model(tensor_data)
        result = output.detach().cpu().numpy()[0]
        reshaped_outputs = result.reshape(-1,result.shape[-2] ,result.shape[-1])
        result = np.sum(reshaped_outputs , axis=1)
        scaler_key = f"scaler_{model_key}"

        if scaler_key in scalers_dict:
            scaler = scalers_dict[scaler_key]
            tensor_data = scaler.inverse_transform(result)
        else:
            tensor_data = minmaxscaler.inverse_transform(result)
        
        tensor_data = tensor_data.astype(int) 
        return tensor_data
    
def get_random_jsons(json_list, weights, k=5):
    
    """
    Selects 'k' number of JSONs from a given list based on the provided weights.

    :param json_list: List of JSONs to choose from.
    :param weights: Weights corresponding to the probabilities of each JSON being chosen.
    :param k: Number of JSONs to select.
    :return: List of selected JSONs.
    """
        
    chosen_json = np.random.choice(json_list, size=k, replace=False, p=weights)
    return chosen_json.tolist()

class PositionalEncoding(nn.Module):
    """
    Adds positional encoding to the input sequence to preserve the order of the sequence.
    """
    def __init__(self, d_model, dropout=0.1, max_len=60):
        super(PositionalEncoding, self).__init__()
        self.dropout = nn.Dropout(p=dropout)
        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model))
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0).transpose(0, 1)
        self.register_buffer('pe', pe)

    def forward(self, x):
        """
        Forward pass of the positional encoding layer.
        """
        x = x + self.pe[:x.size(1)].squeeze(1).unsqueeze(0)
        return self.dropout(x)

class TransformerPredictor(nn.Module):
    """
    Transformer based model for predictions.
    """
    def __init__(self, input_dim, d_model, nhead, num_encoder_layers, output_dim):
        super(TransformerPredictor, self).__init__()
        self.embedding = nn.Linear(input_dim, d_model)
        self.positional_encoding = PositionalEncoding(d_model)
        self.transformer = nn.Transformer(d_model, nhead, num_encoder_layers)
        self.fc = nn.Linear(d_model, output_dim)

    def forward(self, x):
        """
        Forward pass of the transformer predictor.
        """
        x = self.embedding(x)
        x = self.positional_encoding(x)
        x = self.transformer.encoder(x)
        x = self.fc(x)  
        return x
    
def load_models():
    """
    Loads pre-trained models from a specified directory.

    :return: Dictionary of loaded models.
    """
    models = {}
    model_dir = "/home/aiadmin1/Desktop/OMAN_AI_PROJECT/Transformer44M_2"
    for i in range(1, 64):
        model = torch.load(f"{model_dir}/model_{i}.pt", map_location='cpu')
        if isinstance(model, torch.nn.DataParallel):
            model = model.module
        models[f"model_{i}"] = model
    print("> Models loaded")
    return models

# Load models from the filesystem.
models = load_models()

# Define the path where the pickled scalers are stored.
directory_path = "/home/aiadmin1/Desktop/OMAN_AI_PROJECT/minmax/"

# Initialize an empty dictionary to store the loaded scalers.
scalers_dict = {}

# Iterate over each file in the directory_path.
for filename in os.listdir(directory_path):
    
    # Check if the file starts with 'scaler_' and ends with '.pkl'.
    if filename.startswith('scaler_') and filename.endswith('.pkl'):
        identifier = filename.split('_')[1].split('.')[0]
        key_name = f"Scaler_{identifier}"

        # Load the scaler from the pickle file and store it in the scalers_dict.
        with open(os.path.join(directory_path, filename), 'rb') as file:
            scaler = pickle.load(file)
        scalers_dict[key_name] = scaler

print("> scalers loaded")

# Define configuration settings for the Spark application.
checkpoint_dir="/home/aiadmin1/Desktop/OMAN_AI_PROJECT/AktharNaveed/AktharNaveed/checkpoint"
spark_sql_kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
spark_app_name = "SparkKafkaApp"
time_parser_policy = "LEGACY"
log_timestamp = 0
event_type = "N/A"

# Define schemas for parsing data.
after_schema = StructType([
    StructField('CITIZENID', IntegerType(), True),StructField('Age', IntegerType(), True),StructField('SEX', IntegerType(), True),StructField('VOTE_REG_YR', IntegerType(), True),
    StructField('LOG_TIMESTAMP', LongType(), True),StructField('REGIONID', IntegerType(), True),StructField('WILAYATID', IntegerType(), True)
    ])

source_schema = StructType([
    StructField('version', StringType(), True),StructField('connector', StringType(), True),StructField('name', StringType(), True),StructField('ts_ms', LongType(), True),
    StructField('snapshot', StringType(), True),StructField('db', StringType(), True),StructField('sequence', StringType(), True),StructField('schema', StringType(), True),
    StructField('table', StringType(), True),StructField('change_lsn', StringType(), True),StructField('commit_lsn', StringType(), True),StructField('event_serial_no', LongType(), True)
])

transaction_schema = StructType([
    StructField('id', StringType(), True),StructField('total_order', LongType(), True),StructField('data_collection_order', LongType(), True)
])

payload_schema = StructType([
    StructField('before', after_schema, True),StructField('after', after_schema, True),StructField('source', source_schema, True),StructField('op', StringType(), True),
    StructField('ts_ms', LongType(), True),StructField('transaction', transaction_schema, True)
])

schema = StructType([
    StructField('schema', StringType(), True),
    StructField('payload', payload_schema, True)
])

# Function to send a request to a specified endpoint.
def send_request(endpoint, payload,epoch_id=0):
    url = f'https://oci.aioman.org/api/{endpoint}?token=b83eb738224d31e704ad06329edf5cdce3494aedfd65c299e126ac1dbdcce44bfac7c687c069fac7aac2fa730a06a8943493'
    headers = {'Content-Type': 'application/json'}
    data_json = json.dumps(payload)
    try:
        response = requests.post(url, headers=headers, data=data_json)
        print(f'Epoch: {epoch_id} | Response: {response.text}')
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {str(e)}')

# Function to initialize and return a SparkSession with specific configurations.
def launch_spark():
    spark = (
        SparkSession.builder.appName(spark_app_name).config("spark.jars.packages", spark_sql_kafka_package).config("spark.executor.memory", "25g")
        .config("spark.driver.memory", "25g").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.executor.cores", "8").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", time_parser_policy)
    spark.conf.set("spark.sql.shuffle.partitions", "100")
    spark.conf.set("spark.sql.legacy.charVarcharAsString", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
    spark.conf.set("spark.executor.resource.gpu.amount", "4")
    return spark

# Function to remove and recreate the checkpoint directory.
def remove_create_checkpoint_dir():
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
    os.makedirs(checkpoint_dir)


# Function to calculate and cache predictions.
# This function groups data by WilayatID, applies a UDF (not provided in the initial code),
# and updates/creates a table with prediction data.
def calculate_and_cache_predictions(df, conn):
    print("> prediction started")
    predictions_df = df.groupby("WilayatID").apply(process_group_udf).toPandas()
    table_name = "prediction"
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table_name}'")
    table_exists = cursor.fetchone()
    time_slot = (datetime.datetime.now() + datetime.timedelta(hours=1)).replace(minute=0, second=0)

    if not table_exists: 
        cursor.execute(f"""CREATE TABLE {table_name} (RegionID INT,WilayatID INT,TimeSlot DATETIME,Male INT DEFAULT 0,Female INT DEFAULT 0,col_1 INT DEFAULT 0,col_2 INT DEFAULT 0,col_3 INT DEFAULT 0,col_4 INT DEFAULT 0,col_5 INT DEFAULT 0,col_6 INT DEFAULT 0)""")
        region_wilayat_pairs =  [(1, 3), (1, 6), (1, 1), (1, 4), (1, 5), (1, 2),(2, 9), (2, 12), (2, 7), (2, 10), (2, 13), (2, 11),(3, 21), (3, 19), (3, 22), (3, 20),
        (4, 23), (4, 61), (4, 25),(5, 26), (5, 27), (5, 24),(6, 29), (6, 32), (6, 35), (6, 30), (6, 62), (6, 33), (6, 31), (6, 34), (6, 28),(7, 63), (7, 38), 
        (7, 41), (7, 39), (7, 45), (7, 40), (7, 37),(8, 49), (8, 50), (8, 47), (8, 48),(9, 52), (9, 55), (9, 58), (9, 56), (9, 59), (9, 53), (9, 60), (9, 54), 
        (9, 57), (9, 51),(10, 15), (10, 18), (10, 16), (10, 17), (10, 14), (10, 8),(11, 46), (11, 43), (11, 44), (11, 36), (11, 42)]
        for region_id, wilayat_id in region_wilayat_pairs:
            cursor.execute(f"""INSERT INTO {table_name} (RegionID, WilayatID, TimeSlot) VALUES (?, ?, ?)""", (region_id, wilayat_id, time_slot))
        
    for _, row in predictions_df.iterrows():
        cursor.execute(f"SELECT Male, Female, Age_21_30, Age_31_40, Age_41_50, Age_51_60, Age_61_70, Age_71_P FROM Wilayat_Summary WHERE WILAYATID = ?", (row.get('WilayatID'),))
        summary_values = cursor.fetchone()
        if summary_values:
            sql_command = f"""UPDATE {table_name} SET TimeSlot = ?, Male = ?, Female = ?, col_1 = ?, col_2 = ?, col_3 = ?, col_4 = ?, col_5 = ?, col_6 = ? WHERE RegionID = ? AND WilayatID = ?"""
            params = (time_slot, row.get('Male', 0) + summary_values[0], row.get('Female', 0) + summary_values[1], row.get('col_1', 0) + summary_values[2], row.get('col_2', 0) + summary_values[3], 
                row.get('col_3', 0) + summary_values[4], row.get('col_4', 0) + summary_values[5], row.get('col_5', 0) + summary_values[6] , row.get('col_6', 0) + row.get('col_7', 0) + summary_values[7], row.get('RegionID'), row.get('WilayatID'))
            cursor.execute(sql_command, params)

    conn.commit()
    return predictions_df

# Function to get the latest offsets for a given Kafka topic.
def get_latest_offsets(topic):
    consumer = KafkaConsumer(bootstrap_servers='127.0.0.1:9092',group_id=None,enable_auto_commit=False)
    partitions = consumer.partitions_for_topic(topic)
    if partitions is None:
        raise Exception(f"No partitions for topic {topic}")
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)
    latest_offsets = {tp: consumer.position(tp) for tp in topic_partitions}
    consumer.close()
    return latest_offsets

def check_new_data_in_kafka():
    """
    Checks if there's new data available in the Kafka topic.

    Returns:
        bool: True if new data has arrived, False otherwise.
    """

    consumer = KafkaConsumer('server1.MOI_AI.dbo.Election_Data',
                             bootstrap_servers='127.0.0.1:9092',
                             auto_offset_reset='latest',
                             group_id=None,
                             enable_auto_commit=False,
                             fetch_min_bytes=1,
                             fetch_max_wait_ms=25000,
                             max_poll_records=500000
                             ) 
    
    new_data_arrived = False
    poll_timeout_ms = 25000 
    messages = consumer.poll(timeout_ms=poll_timeout_ms)
    if messages:
        new_data_arrived = True
    consumer.close()
    return new_data_arrived

def read_batch(spark):
    """
    Reads, processes, and sends data to an API.

    Args:
        spark (SparkSession): Spark session instance.

    Returns:
        None.
    """
    topic = 'server1.MOI_AI.dbo.Election_Data'
    last_processed_offsets = get_latest_offsets(topic)
    start_time = time.time()

    while True:

        elapsed_time = time.time() - start_time
        if elapsed_time >= 25:
            print(f"> waiting for data to be flushed at {time.strftime('%H:%M:%S', time.localtime())}")
            start_time = time.time()  

        if check_new_data_in_kafka():
            # Check for new data in Kafka and process if available.

            print("> waiting for data to be read")
            time.sleep(20)

            start_time = time.time()
            latest_offsets = get_latest_offsets(topic)

            # Loading data from Kafka.
            starting_offsets = {tp.topic: {str(tp.partition): last_processed_offsets[tp]} for tp in last_processed_offsets}
            ending_offsets = {tp.topic: {str(tp.partition): latest_offsets[tp]} for tp in latest_offsets}
            batch_input_df = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", "127.0.0.1:9092").option("subscribe", topic)
                .option("startingOffsets", json.dumps(starting_offsets)).option("endingOffsets", json.dumps(ending_offsets))
                .load()
            )
            
            # Data transformations.
            string_df = batch_input_df.selectExpr("CAST(value AS STRING)")
            
            # Assuming schema is predefined.
            json_df = string_df.select(from_json(col("value"), schema).alias("parsed_value"))

            #selecting df with specified columns
            df = json_df.select(
                F.col('parsed_value.payload.after.CITIZENID'),F.col('parsed_value.payload.after.Age'),F.col('parsed_value.payload.after.SEX'),F.col('parsed_value.payload.after.VOTE_REG_YR'),
                F.col('parsed_value.payload.after.LOG_TIMESTAMP'),F.col('parsed_value.payload.after.REGIONID'),F.col('parsed_value.payload.after.WILAYATID')
            )

            #Renaming the spark columns
            df = df.withColumn('Time', F.date_format(F.from_unixtime(F.col('LOG_TIMESTAMP') / 1000), 'HH:mm:ss')) \
                .withColumn('IDCardNo', F.col('CITIZENID')).withColumn('RegionID', F.col('REGIONID')) \
                .withColumn('WilayatID', F.col('WILAYATID'))\
                .withColumn('Male', F.when(F.col('SEX') == 1, 1).otherwise(0)).withColumn('Female', F.when(F.col('SEX') == 2, 1).otherwise(0))

            #creating age columns
            age_bins = [(20, 30), (31, 40), (41, 50), (51, 60), (61, 70), (71, 80),(81, 90)]
            for age_bin in age_bins:
                df = df.withColumn(f'{age_bin[0]}_{age_bin[1]}', F.when((F.col('Age')>=age_bin[0]) & (F.col('Age')<age_bin[1]), 1).otherwise(0))
            
            df_select = df.select('Time','RegionID', 'WilayatID', 'Male', 'Female', *[f'{age_bin[0]}_{age_bin[1]}' for age_bin in age_bins])
            df = df_select.withColumnRenamed('20_30', 'col_1').withColumnRenamed('31_40', 'col_2').withColumnRenamed('41_50', 'col_3') \
            .withColumnRenamed('51_60', 'col_4').withColumnRenamed('61_70', 'col_5').withColumnRenamed('71_80', 'col_6').withColumnRenamed('81_90', 'col_7')
            
            print("> table count before processing ",df.count())

            df = df.withColumn('hour', F.hour("Time"))
            df = df.withColumn('minute', F.minute("Time"))
            df = df.withColumn('second', F.second("Time"))
            df = df.withColumn("adjusted_minute", F.when(F.col("second") >= 30, F.col("minute") + 1).otherwise(F.col("minute")))
            df = df.withColumn("Time", F.concat(F.format_string("%02d", F.col("hour")), F.lit(":"), F.format_string("%02d", F.col("adjusted_minute"))))
            df = df.drop("hour", "minute", "second", "adjusted_minute")

            df = df.repartition(100, ['WilayatID'])
            df = df.sortWithinPartitions(['RegionID', 'WilayatID'])
            sum_cols = ['col_' + str(i) for i in range(1, 8)] + ['Male', 'Female']
            df = df.groupby('RegionID', 'WilayatID', 'Time').agg(*[F.sum(c).alias(c) for c in sum_cols])

            if "IDCardNo" in df.columns:  
                df = df.drop("IDCardNo") 
            df = df.dropna()
          
            calculate_and_cache_predictions(df,conn)

            print("> API sending started")

            #Graph function for region Wilayat
            def process_region_wilayat(id_value, level, conn):
                return [regionwilayat_pollingstats(id_value, level,conn),regionwilayat_genderstats(id_value, level, conn),combined_regionwilayat_polling(id_value, level,conn),
                        regionwilayatgender_prediction(id_value, level, conn),regionwilayatageInterval_stats(id_value, level, conn)]
            
            #graph for OMAN
            json_list_1 = [lambda: agewisePolling_stats(conn),lambda: regionwise_gender_stats(conn),lambda: wilayatwise_gender_stats(conn),lambda: Ageinterval_stats(conn),
                           lambda: Registered_year_wise_stats(conn),lambda: Regionwise_stats(conn),lambda: regionwiseage_stats(conn),lambda: wilayatwiseage_stats(conn)]
            json_list_2  = [lambda: genderwise_Prediction(conn),lambda: agewise_Prediction(conn)]

            #weighted selection of OMAN graph
            weights_1 = [0.1, 0.2 ,0.2 , 0.2 ,0.1, 0.1, 0.05, 0.05]
            weights_2 = [0.5 , 0.5]

            selected_indices_1 = np.random.choice(len(json_list_1), size=1, replace=False, p=weights_1)
            selected_indices_2 = np.random.choice(len(json_list_2), size=1, replace=False, p=weights_2)

            json_oman = [pollingstats(conn),genderstats(conn),combined_polling_data(conn)]+[json_list_1[i]() for i in selected_indices_1]+[json_list_2[i]() for i in selected_indices_2]
            json_oman = {"oman": json_oman}
            
            #API senting for OMAN
            send_request('generate_script_oman', json_oman) 
            send_request('generate_script_oman_en', json_oman)
            send_request('generate_script_oman_cn', json_oman)
            send_request('generate_script_oman_ru', json_oman)
            send_request('generate_script_oman_fr', json_oman)
            send_request('generate_script_oman_es', json_oman)
           
            #API senting for region 
            unique_regions, json_region = range(1,12), {}
            for id in unique_regions:
                json_region[f"{id}"] = process_region_wilayat(id, 'RegionID', conn)
            send_request('generate_script_region', json_region)

            #API senting for wilayat
            unique_wilayats, json_wilayat = range(1,64), {}
            for id in unique_wilayats:
                json_wilayat[f"{id}"] = process_region_wilayat(id, 'WilayatID', conn)
            send_request('generate_script_wilayat', json_wilayat)
            last_processed_offsets = latest_offsets

            end_time = time.time()
            execution_time = end_time - start_time
            print(f"> process finished in {execution_time:.2f} seconds.")

if __name__ == "__main__":
    """
    Main execution logic.
    """
    # Manage the checkpoint directory.
    remove_create_checkpoint_dir()
    # Launch Spark session.
    spark = launch_spark()
    # Start reading and processing the batch.
    read_batch(spark)
    # Close connection (presumably a DB connection).
    conn.close()
    