#!/usr/bin/env python
# coding: utf-8

# In[81]:


# Importing Packages

import numpy as np
import pandas as pd
import time
import six
import sys
import requests
import subprocess
import re
from google.cloud import bigquery
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics
from matplotlib import pyplot as plt
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import RandomForestRegressor


# In[82]:


# Starting and Creating Spark Session

sc = SparkContext.getOrCreate()
start = time.time()
sqlContext = SQLContext(sc)


# In[83]:


###################
# Importing TCI/VCI Data and Output Variable of Interest
###################
csv_path = "gs://bigdatabucketcolumbia/final/data/maine/finalmainedatanick-tcitest.csv"


# In[84]:


# Loading Data

data = spark.read.csv(csv_path, header="true", inferSchema="true")
data.show()


# In[85]:


data.take(1)


# In[86]:


data.cache()
data.printSchema()


# In[87]:


data.describe().toPandas()


# In[88]:


# Vector Assembler
    
feature_columns = data.columns[:-1] # Output 
vectorAssembler = VectorAssembler(inputCols=feature_columns,outputCol="features")
vdata = vectorAssembler.transform(data)
vdata = vdata.select(['features', 'tci'])
vdata.show(3)


# In[89]:


splits = vdata.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]


# In[90]:


lr = LinearRegression(featuresCol = 'features', labelCol='tci', maxIter=20, regParam=0.01, elasticNetParam=0.1)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))


# In[91]:


trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)


# In[92]:


train_df.describe().show()


# In[93]:


lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction","tci","features").show(5)
from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction",                  labelCol="tci",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))


# In[94]:


test_result = lr_model.evaluate(test_df)
print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)


# In[95]:


print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()


# In[96]:


# Linear Regression Model
predictions = lr_model.transform(test_df)
#predictions.select("prediction","tci","features").show(3)
predictions.select("features","tci", "prediction").show(3)


# In[97]:


#################################################

# Decision Tree Regression
# https://towardsdatascience.com/building-a-linear-regression-with-pyspark-and-mllib-d065c3ba246a

from pyspark.ml.regression import DecisionTreeRegressor
dt = DecisionTreeRegressor(featuresCol ='features', labelCol = 'tci')
dt_model = dt.fit(train_df)
dt_predictions = dt_model.transform(test_df)
dt_evaluator = RegressionEvaluator(labelCol="tci", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)


# In[98]:


dt_model.featureImportances


# In[99]:


data.take(1)


# In[100]:


##############################################

# Gradient-Boosted Tree Regression

gbt = GBTRegressor(featuresCol = 'features', labelCol = 'tci', maxIter=10)
gbt_model = gbt.fit(train_df)
gbt_predictions = gbt_model.transform(test_df)
gbt_predictions.select('features', 'tci', 'prediction').show(3)


# In[101]:


gbt_evaluator = RegressionEvaluator(labelCol="tci", predictionCol="prediction", metricName="rmse")
rmse = gbt_evaluator.evaluate(gbt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)


# In[102]:


#########################################

# Random Forest Regression

rf = RandomForestRegressor(featuresCol="features", labelCol = 'tci')
rf_model = rf.fit(train_df)
rf_predictions = gbt_model.transform(test_df)
rf_predictions.select('features','prediction', 'tci' ).show(5)


# In[103]:


rf_evaluator = RegressionEvaluator(labelCol="tci", predictionCol="prediction", metricName="rmse")
rmse = rf_evaluator.evaluate(gbt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

