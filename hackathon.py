import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import os.path

# set this to True to force download database using SQL,
# else {if `datafile` exists, load it. else download from database}
download = False
datafile = 'data2.csv'
engine = None

if download or not os.path.isfile(datafile):
    if engine is None:
        print('Creating database engine...')
        engine = create_engine('mysql+pymysql://iotr:iotr123@115.88.201.51/kisti')
    print('Querying database...')
    df = pd.read_sql_query('SELECT * FROM sensorParser WHERE gateway_id="SERVER"\
                            AND timestamp IS NOT NULL AND timestamp <> "" ORDER BY timestamp LIMIT 5000000', engine)
    print('Saving "{}" to disk...'.format(datafile))
    df.to_csv(datafile)
else:
    print('Reading from "{}"...'.format(datafile))
    df = pd.read_csv(datafile, header=0)
df_temp = df
df.head()
df = df.iloc[3:]  #remove index in table
print(df.head(5))

#m7.timestamp[df.timestamp < '2017-08']

########## split date #########

m7 = df[df.timestamp >= '2017-07']
m7 = m7[m7.timestamp < '2017-08']


##### remove outlier #########
m7 = m7[m7.so2_value < 20]
m7 = m7[m7.co_value < 10]
#m7 = m7[m7.no2_value < 0.02]


############ Filter ##########

so2 = m7.so2_value
so2.hist()
no2 = m7.no2_value
co2 = m7.co_value
temp = m7.temp_value
hum = m7.hum_value
pres = m7.pres_value
pm25 = m7.pm2_5_value
pm10 = m7.pm10_value



####### norm data ########

norm_so2 = ( m7.so2_value - m7.so2_value.min() )/( m7.so2_value.max() - m7.so2_value.min())
m7['norm_so2'] = norm_so2 
norm_no2 = ( m7.no2_value - m7.no2_value.min() )/( m7.no2_value.max() - m7.no2_value.min())
m7['norm_no2'] = norm_no2
norm_co2 = ( m7.co_value - m7.co_value.min() )/( m7.co_value.max() - m7.co_value.min())
m7['norm_co2'] = norm_co2
norm_pm25 = ( pm25 - pm25.min() )/( pm25.max() - pm25.min())
m7['norm_pm25'] = norm_pm25
norm_pm10 = ( pm10 - pm10.min() )/( pm10.max() - pm10.min())
m7['norm_pm10'] = norm_pm10
norm_hum = ( hum - hum.min())/( hum.max() - hum.min())
m7['norm_pm10'] = norm_hum



###### group ######
m7['timegroup'] = m7.timestamp.apply(lambda x: x.split()[0])
grouped = m7.groupby('timegroup')
so2mean = grouped.norm_so2.mean()
no2mean = grouped.norm_no2.mean()
co2mean = grouped.norm_co2.mean()
pm25mean = grouped.norm_pm25.mean()
pm10mean = grouped.norm_pm10.mean()


##### norm ######

so2mean = ( so2mean - so2mean.min() )/( so2mean.max() - so2mean.min())
no2mean = ( no2mean - no2mean.min() )/( no2mean.max() - no2mean.min())
co2mean = ( co2mean - co2mean.min() )/( co2mean.max() - co2mean.min())
pm25mean = ( pm25mean - pm25mean.min() )/( pm25mean.max() - pm25mean.min())
pm10mean = ( pm10mean - pm10mean.min() )/( pm10mean.max() - pm10mean.min())


###### sum in month #######
Sumpm25 = pm25.cumsum()
m7['Sumpm_25'] = Sumpm25
Sumpm10 = pm10.cumsum()
m7['Sumpm_10'] = Sumpm10



###### Plot Graph #######

x = np.linspace(1, 31, num=31)


#### pm25  ####

plt.plot(x, pm25mean, '-', linewidth=2)
plt.legend(['pm25'])

#### pm10  ####

plt.plot(x, pm25mean, '-', linewidth=2)
plt.legend(['pm10'])


#### so2  ####

plt.plot(x, so2mean, '-', linewidth=2)
plt.legend(['so2'])

#### no2  ####

plt.plot(x, no2mean, '-', linewidth=2)
plt.legend(['no2'])

#### co2  ####

plt.plot(x, co2mean, '-', linewidth=2)
plt.legend(['co2'])


#### so2 and no2 ####

plt.plot(x, so2mean, '-', linewidth=2)
plt.plot(x, no2mean, '-', linewidth=2)
plt.legend(['so2', 'no2'])

#### so2 and co2 ####

plt.plot(x, so2mean, '-', linewidth=2)
plt.plot(x, co2mean, '-', linewidth=2)
plt.legend(['so2', 'co2'])

#### pm25 and pm10 ####

plt.plot(x, pm25mean, '-', linewidth=2)
plt.plot(x, pm10mean, '-', linewidth=2)
plt.legend(['pm25', 'pm10'])

#### pm25 , pm10 and so2 ####

plt.plot(x, pm25mean, '-', linewidth=2)
plt.plot(x, pm10mean, '-', linewidth=2)
plt.plot(x, so2mean, '-', linewidth=2)
plt.legend(['pm25', 'pm10','so2'])

#### pm25 , pm10 and no2 ####

plt.plot(x, pm25mean, '-', linewidth=2)
plt.plot(x, pm10mean, '-', linewidth=2)
plt.plot(x, no2mean, '-', linewidth=2)
plt.legend(['pm25', 'pm10','no2'])

#### pm25 , pm10 and co2 ####

plt.plot(x, pm25mean, '-', linewidth=2)
plt.plot(x, pm10mean, '-', linewidth=2)
plt.plot(x, co2mean, '-', linewidth=2)
plt.legend(['pm25', 'pm10','co2'])


plt.show()



#fig = plt.gcf()

    
##### write file ######

#m7.to_csv('sensorData2.csv')



#print(df.gateway_id)
#size = df.shape
#print(size)