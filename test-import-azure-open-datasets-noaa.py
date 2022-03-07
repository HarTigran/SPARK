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

display(psdf)

# COMMAND ----------

location = psdf[['latitude','longitude']]

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

geometry = [Point(xy) for xy in zip(psdf['longitude'].to_numpy(),psdf['latitude'].to_numpy())]

# COMMAND ----------

crs = {'init':'epsg:4326'}

# COMMAND ----------

geo_df = gpd.GeoDataFrame(psdf,
                          crs = crs,
                          geometry = geometry
)

# COMMAND ----------

geometry_df.head()

# COMMAND ----------

fig, ax = plt.subplots(figsize = (15,15))
street_map.plot(ax=ax, alph = 0.4, color = 'grey')
geo_df[geo_df['status'] == 'Canceled'].plot(ax = ax, markersize = 20, color = 'blue')
