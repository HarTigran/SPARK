# Databricks notebook source
from azureml.opendatasets import ChicagoSafety

from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta

# COMMAND ----------

isd = ChicagoSafety(datetime(2001, 1, 1, 0, 0),datetime(2022, 2, 22, 0, 0),None,True)
df = isd.to_spark_dataframe()
display(df.limit(10))

# COMMAND ----------

#upload more datasets (do tab complete)
#from azureml.opendatasets import 

# COMMAND ----------

!pip install shapely

# COMMAND ----------

import numpy as np
import pyspark.pandas as ps
import matplotlib.pyplot as plt
import descartes
import geopandas as gpd
from shapely.geometry import Point, Polygon
%matplotlib inline

# COMMAND ----------

psdf = ps.DataFrame(df)

# COMMAND ----------

psdf.describe()

# COMMAND ----------

display(psdf)

# COMMAND ----------

psdf = psdf[psdf['latitude'].notna()]

# COMMAND ----------

psdf.category.unique()

# COMMAND ----------

location = psdf[['latitude','longitude', 'status','dateTime','category' ]]

# COMMAND ----------

location.isna().sum()

# COMMAND ----------

mp = '/dbfs/FileStore/shared_uploads/th314@duke.edu/geo_export_ba35e27a_5ee8_4e54_b04c_2abb6a2760e5.shp'
street_map = gpd.read_file(mp, encoding="utf-8")
# ldn = street_map.plot()
# display(ldn.figure)
fig, ax = plt.subplots(figsize = (15,15))
street_map.plot(ax=ax)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

geometry = [Point(xy) for xy in zip(location['longitude'].to_numpy(),location['latitude'].to_numpy())]


# COMMAND ----------

location = location.to_pandas()
crs = {'init':'epsg:4326'}

# COMMAND ----------

geo_df = gpd.GeoDataFrame(location,
                          crs = crs,
                          geometry =geometry
                         )

# COMMAND ----------

geo_df.head()

# COMMAND ----------

fig, ax = plt.subplots(figsize = (15,15))
street_map.plot(ax=ax, alpha = 0.4, color = 'grey')
geo_df[(geo_df['status'] == 'Completed') &(geo_df['category'] == 'Aircraft Noise Complaint') ].plot(ax = ax, markersize = 5, color = 'blue')

# COMMAND ----------

fig, ax = plt.subplots(figsize = (15,15))
street_map.plot(ax=ax, alpha = 0.4, color = 'grey')
geo_df[(geo_df['dateTime'] < pd.Timestamp(2019,5,5)) &(geo_df['category'] == 'Dead Animal Pick-Up Request') ].plot(ax = ax, markersize = 5, color = 'blue')

# COMMAND ----------

location['year'] = location['dateTime'].dt.year.astype(str)

# COMMAND ----------

location.head()

# COMMAND ----------

from cartoframes.viz import *

# COMMAND ----------

from cartoframes.viz import Map,Layer,basemaps

Map(
 basemap=basemaps.voyager,
 layers=[Layer(location.sample(1000)[['dateTime','geometry']],geom_col='geometry',encode_data=False)]
)

# COMMAND ----------

Layer_1 = Layer(
    location.sample(20000),
    #specify geometry column
    geom_col='geometry',
    #when working with large data, set encode_data False
    encode_data=False,
    title="Summary of the Calls",
    widgets= [
              #Show the incident count
              formula_widget(value='dateTime',operation='count',title='Total Calls'),
            
              #create category widget to sort incidents by state
              category_widget(value='category',title='Calls by category'),
        
              #create category widget to sort incidents by state
              category_widget(value='status',title='Calls by status'),
            
              #create category widget to sort incidents by weekday
              category_widget(value='year',title='Calls by year'),
             ],
    
    #this will show killed people count when mouce click
    popup_click=popup_element(value='category')
)

#visualize the map
Map(Layer_1,layer_selector=True)


# COMMAND ----------

Layer_2 = Layer(
    location.sample(20000),
    geom_col='geometry',
  
    #create time series animation for individual incident 
    style = animation_style('dateTime',duration=30),
    encode_data=False,
    title="Time series Layer",
)

#Add layers to Map and set basemap as darkmatter
map_viz = Map(
  layers=[
    Layer_1,
    Layer_2],
  basemap=basemaps.darkmatter,
  title='Calls',
  layer_selector=True)

# COMMAND ----------


