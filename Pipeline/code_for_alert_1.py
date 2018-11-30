
# coding: utf-8

# In[1]:


import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
plt.rc("font", size=14)

import datetime
import threading, logging, time
import multiprocessing

import math

from kafka import KafkaConsumer, KafkaProducer


# In[98]:


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        while not self.stop_event.is_set():
            producer.send('my-topic', b"test")
            producer.send('my-topic', b"\xc2Hola, mundo!")
            time.sleep(1)

        producer.close()

class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        
    def stop(self):
        self.stop_event.set()
        
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['my-topic'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()
        
        
def main():
    tasks = [
        Producer(),
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)
    
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()
        
        
if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()


# In[20]:


# comsumtion_value data for last one minute
train = pd.read_csv("D:/datascience/kafka/household.csv")

# stores mean and standard deviation calculated from previuos data for each household device
alert_record=pd.read_csv("D:/datascience/kafka/alert_1_previous_record.csv")

# stores real time alert status of device
alert_status=pd.read_csv("D:/datascience/kafka/current_alert_1_status.csv")


# In[15]:


#convert timestamp to datetime format

train['datetime']=pd.to_datetime(train['timestamp'], utc='GMT',unit='s')


# In[16]:


#extract year, month, day, hour from datetime

def split_data(train):
    train['record_year']=train['datetime'].dt.year
    train['record_month']=train['datetime'].dt.month
    train['record_day']=train['datetime'].dt.day
    train['record_hour']=train['datetime'].dt.hour
    train.drop('timestamp',axis=1,inplace=True)
    train.drop('datetime',axis=1,inplace=True)
    return train

train=split_data(train)


# In[11]:


train.head(2)


# In[12]:


alert_record.head(2)


# In[13]:


alert_status.head(2)


# In[ ]:


#rum program for each comsumtion_value that passed through kafka in last one minute

for i in range(len(train)):
    
# storing variables for each row----------------
    
    house=int(train.loc[i, 'house_id'])
    household=int(train.loc[i, 'household_id'])
    year=int(train.loc[i, 'record_year'])
    month=int(train.loc[i, 'record_month'])
    day=int(train.loc[i, 'record_day'])
    hour=int(train.loc[i, 'record_hour'])
    consumption_value=train.loc[i, 'value']
    
    
# drop value variables for row when consumption value is either zero or empty

    if float(consumption_value)==0:
        continue

        
# import mean and standard deviation from alert_record file to impute current alert status of device

    mean_upto=alert_record.loc[(alert_record['house_id']==house) & (alert_record['household_id']==household) &  (alert_record['record_hour']==hour),'mean']
    standard_deviation_upto=alert_record.loc[(alert_record['house_id']==house) & (alert_record['household_id']==household) &  (alert_record['record_hour']==hour),'standard_deviation']
    count_upto=alert_record.loc[(alert_record['house_id']==house) & (alert_record['household_id']==household) &  (alert_record['record_hour']==hour),'count']

    
# check current alert_status of device

    alert=alert_status.loc[ (alert_status['house_id']==house) & (alert_status['household_id']==household) , 'alert_status']
    
    
# if sapient install new device there where will no record so we have to insert that new record of house_id,
#   household_id in our alert record file that stores mean and standard devaition

    if(mean_upto.empty):
        df_alert_record = pd.DataFrame([[house,household,hour,0,0,0]], columns=['house_id','household_id','record_hour','mean','standard_deviation','count'])
        alert_record=alert_record.append(df_alert_record, ignore_index=True)
    
        mean_upto=0
        standard_deviation_upto=0
        count_upto=0
        
# similiarly we have to insert new record of house_id household_id in alert_status_file that shows current status of device  

    if(alert.empty):
        df_alert_status = pd.DataFrame([[house,household,0]], columns=['house_id','household_id','alert_status'])
        alert_status=alert_status.append(df_alert_status, ignore_index=True)
        alert= 0
                  
    
# drop consumption_value variables for row when consumption value is very large

    if consumption_value> float(mean_upto) + 3*float(standard_deviation_upto):
        continue
    
# for old device we can compute current alert_status of device

    if consumption_value > float(mean_upto) + float(standard_deviation_upto):
        alert_status[ (alert_status['house_id']==house) & (alert_status['household_id']==household), 'alert_status']=1  
        
    else:
        alert_status[ (alert_status['house_id']==house) & (alert_status['household_id']==household), 'alert_status']=0

        
#update mean_upto and standard deviation to make computation for next consumtion_value

    new_count=count_upto+1
    
    new_mean=((mean_upto*count_upto) + consumption_value)/(new_count)
    
    new_std_deviation=math.sqrt((count_upto*(standard_deviation_upto**2)+(mean_upto-consumption_value)*(new_mean-consumption_value))/new_count)
    
#update these values in alert record file for particluar device 
    alert_record.loc[(alert_record['house_id']==house) & (alert_record['household_id']==household) &  (alert_record['record_hour']==hour),'mean']=new_mean
    alert_record.loc[(alert_record['house_id']==house) & (alert_record['household_id']==household) &  (alert_record['record_hour']==hour),'standard_deviation']=new_std_deviation
    alert_record.loc[(alert_record['house_id']==house) & (alert_record['household_id']==household) &  (alert_record['record_hour']==hour),'count']=new_count
    


# In[174]:


#update our csv files.

alert_record.to_csv("D:/datascience/kafka/alert_1_previous_record.csv", index = False)
alert_status.to_csv("D:/datascience/kafka/current_alert_1_status.csv", index = False)

