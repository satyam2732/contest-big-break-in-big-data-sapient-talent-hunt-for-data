
# coding: utf-8

# In[48]:


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


# In[49]:


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


# In[65]:


# comsumtion_value data for last one minute
train = pd.read_csv("D:/datascience/kafka/household.csv")

#store consumption_value of last one hour
last_hour=pd.read_csv("D:/datascience/kafka/alert_2_last hour_record.csv")

# stores real time alert status of device
alert_status=pd.read_csv("D:/datascience/kafka/current_alert_2_status.csv")


# In[66]:


#sort values as per timestamp to make data realtime

train=train.sort_values(['timestamp'])
train = train.reset_index(drop=True)


# In[67]:


train.head(4)


# In[68]:


last_hour.head(5)


# In[61]:


count=0
mean=0
std=0


# In[71]:


for i in range(len(train)):
    
    # storing variables for each row----------------
    
    house=int(train.loc[i, 'house_id'])
    household=int(train.loc[i, 'household_id'])
    time=int(train.loc[i, 'timestamp'])
    consumption_value=train.loc[i, 'value']
    
    
# drop value variables for row when consumption value is either zero or empty
    if float(consumption_value)==0:
        continue

        
# check current alert_status of device
    alert=alert_status.loc[ (alert_status['house_id']==house) & (alert_status['household_id']==household) , 'alert_status']
    
    
# if sapient install new device there where will no record so we have to insert that new record of house_id, household_id in alert_status_file that shows current status of device  

    if(alert.empty):
        df_alert_status = pd.DataFrame([[house,household,0]], columns=['house_id','household_id','alert_status'])
        alert_status=alert_status.append(df_alert_status, ignore_index=True)
        alert= 0
                  
    
# drop consumption_value variables for row when consumption value is very large

    if consumption_value> float(mean) + 3*float(std):
        continue
    
# for old device we can compute current alert_status of device

    if consumption_value > float(mean_upto) + float(standard_deviation_upto):
        alert_status[ (alert_status['house_id']==house) & (alert_status['household_id']==household), 'alert_status']=1  
        
    else:
        alert_status[ (alert_status['house_id']==house) & (alert_status['household_id']==household), 'alert_status']=0

        
#update mean_upto and standard deviation to make computation for next consumtion_value

    new_count=count+1    
    new_mean=((mean*count) + consumption_value)/(new_count)
    new_std_deviation=math.sqrt((count_upto*(std**2)+(mean-consumption_value)*(new_mean-consumption_value))/new_count)
        
    count=new_count
    mean=new_mean
    std=new_std_devaition


# In[72]:


#update our alert 2 csv files.

alert_status.to_csv("D:/datascience/kafka/current_alert_2_status.csv", index = False)

