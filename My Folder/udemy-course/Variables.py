# Databricks notebook source
from datetime import datetime


a = 1
b = 'Hello world!'
c = ['a', 'b', 'c']
date = datetime.now()

# COMMAND ----------

import numpy as np
import pandas as pd
import random 
import pyspark.pandas as ps 

my_np_array = np.random.rand(3,2)
my_pandas_df = pd.DataFrame({"Column1": my_np_array[:, 0],
                             "Column2": my_np_array[:, 1]})
my_koalas_df = ps.from_pandas(my_pandas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Python notebook debugger

# COMMAND ----------

import time
import pandas as pd 


d = {'col1': [1, 2], 'col2': [3, 4]}
df = pd.DataFrame(data=d)

x = 5
time.sleep(6)
x = 6

print("pre foo")
foo(df)
print("done!")

# COMMAND ----------

