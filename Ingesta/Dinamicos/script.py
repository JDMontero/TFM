import pandas as pd
import numpy as np
from kafka import KafkaProducer
import os

#Kafka config

kafka_broker_hostname = "localhost"
kafka_consumer_portno = "9095"
kafka_broker = kafka_broker_hostname + ":" + kafka_consumer_portno
kafka_topic = "test"

producer = KafkaProducer(bootstrap_servers=kafka_broker)

#Leemos el dataset de train procesado
path = "/home/jmontero/TFM/Data/Dynamic/"

for files in os.listdir("/home/jmontero/TFM/Data/Dynamic/"):
    if str.endswith(files,".csv"):
        path+=files

#Train
train = pd.read_csv(path)

#Creamos los que van a ser los datasets generados cada X tiempo para simular
#un comportamiento dinamico en el proyecto

new_values = pd.DataFrame(columns = train.columns)

ids = train["TransactionID"].max() + 1

#Se crearan datos pseudo-aleatorios cada X segundos
import random
import time
n = 500
count = 0
while True:
    new_values_dict = {}

    new_values_dict["TransactionID"] = ids
    ids+=1

    for col in new_values.columns[1:]:

        new_values_dict[col] = train[col].iloc[random.randint(0,train.shape[0]-1)]

    new_values = new_values.append(new_values_dict, ignore_index = True)

    n-=1
    if(n<=0):

    	count+=1
    	print("Nuevos valores generados")
    	print(new_values)
    	n=500

    	for _index in range(0, len(new_values)):

    		json_values = new_values[new_values.index==_index].to_json(orient = 'records')

    		producer.send(kafka_topic, bytes(json_values, 'utf-8'))

    	time.sleep(20)

