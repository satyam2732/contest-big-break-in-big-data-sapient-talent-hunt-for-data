
# coding: utf-8

# In[52]:


#import python libraries

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
plt.rc("font", size=14)

import datetime

#import household data

train = pd.read_csv("D:/datascience/kafka/household.csv")


# In[53]:


#drop rows with consumption_value 0

train = train.drop(train[train.value==0].index)


# In[54]:


#convert timestamp to datetime

train['record_date']=pd.to_datetime(train['timestamp'], utc='GMT',unit='s')


# In[55]:


train.describe()


# In[56]:


#extract year, month, day, hour from record_date column

def split_data(train):
    train['record_year']=train['record_date'].dt.year
    train['record_second']=train['record_date'].dt.second
    train['record_month']=train['record_date'].dt.month
    train['record_day']=train['record_date'].dt.day
    train['record_hour']=train['record_date'].dt.hour
    train['record_minute']=train['record_date'].dt.minute

#drop timestamp, record_date since they are of no use
    train.drop('timestamp',axis=1,inplace=True)
    train.drop('record_date',axis=1,inplace=True)
    train.drop('record_minute',axis=1,inplace=True)
    return train

train=split_data(train)


# In[57]:


#calculate mean consumption value for each hour of every device installled

train=train.groupby(['house_id','household_id','record_year','record_month','record_day','record_hour'],as_index=False)['value'].mean()


# In[58]:


#calculate mean of all the devices for each hour

data_1=train.groupby(['record_year','record_month','record_day','record_hour'],as_index=False)['value'].mean()


# In[59]:


#calculate standard deviation of all the devices for each hour

data_2=train.groupby(['record_year','record_month','record_day','record_hour'],as_index=False)['value'].std()


# In[60]:


#sum up mean and standard devaition as update_value

data_1['update_value']=data_1['value']+data_2['value']

data_1.drop('value',axis=1,inplace=True)


# In[61]:


for i in range(len(train)):
    year=int(train.loc[i, 'record_year'])
    month=int(train.loc[i, 'record_month'])
    day=int(train.loc[i, 'record_day'])
    hour=int(train.loc[i, 'record_hour'])    
    temp=data_1.loc[ (data_1['record_year'] == year) & (data_1['record_month']==month) & (data_1['record_day']==day) & (data_1['record_hour']==hour),'update_value']    
    train.loc[i, 'ans']=float(temp)


# In[62]:


#check whether consumption value is greater than sum of mean and standard deviation or not to get alert status

for i in range(len(train)):
    if(float(train.loc[i, 'value'])>float(train.loc[i, 'ans'])):
        train.loc[i, 'final']=1
    else:
        train.loc[i, 'final']=0


# In[63]:


test2=pd.read_csv("D:/datascience/kafka/test_sZn4Axl/alert_type_2.csv")


# In[64]:


#split id into house_id, household_id, date, hour, day

test2['house_id']=test2['id'].str.split('_').str.get(0)
test2['household_id']=test2['id'].str.split('_').str.get(1)
test2['date']=test2['id'].str.split('_').str.get(2)
test2['hour']=test2['id'].str.split('_').str.get(3)
test2['day']=test2['date'].str.split('-').str.get(0)
test2['month']=test2['date'].str.split('-').str.get(1)
test2['year']=test2['date'].str.split('-').str.get(2)
test2.drop('date',axis=1,inplace=True)


# In[65]:


train['final']=train['final'].astype('int64')


# In[66]:


# check alert status by comparing to above dataframe

for i in range(len(test2)):
    house=int(test2.loc[i, 'house_id'])
    household=int(test2.loc[i, 'household_id'])
    year=int(test2.loc[i, 'year'])
    month=int(test2.loc[i, 'month'])
    day=int(test2.loc[i, 'day'])
    hour=int(test2.loc[i, 'hour'])    
    temp=train.loc[(train['house_id'] == house) & (train['household_id'] == household) & (train['record_year'] == year) & (train['record_month']==month) & (train['record_day']==day) & (train['record_hour']==hour),'final']   
    
    if(temp.empty==False):    
        if(int(temp)==1):
            test2.loc[i, 'alert']=1
        else:
            test2.loc[i, 'alert']=0


# In[67]:


test2.drop('house_id',axis=1,inplace=True)
test2.drop('household_id',axis=1,inplace=True)
test2.drop('hour',axis=1,inplace=True)
test2.drop('year',axis=1,inplace=True)
test2.drop('day',axis=1,inplace=True)
test2.drop('month',axis=1,inplace=True)


# In[ ]:


#import alert status.

test2.to_csv("D:/datascience/kafka/test_sZn4Axl/test_2.csv")

