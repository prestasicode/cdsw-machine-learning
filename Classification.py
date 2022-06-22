#Import Some Library / Package
import pandas as pd
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import GBTClassifier
from pyspark.sql.functions import col, explode, array, lit
from pyspark.sql.functions import date_format, trim, concat_ws, when, split, regexp_replace
from pyspark.ml.feature import VectorSlicer
from pyspark.sql import functions as F
from pyspark.sql import SparkSession 

#Create a Session Spark
spark = SparkSession.builder.appName("Testing").getOrCreate()
spark.conf.set('spark.sql.caseSensitive', False)

#Load Model
from pyspark.ml.classification import GBTClassificationModel
model_path_gbt = "model_gbt"
gbtModel = GBTClassificationModel.load(model_path_gbt)

data = spark.sql("""
*query_data
""")

data = data.drop()

#Preprocessing Data
categoricalColumns = [__]
stages = []

for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    stages += [stringIndexer, encoder]

# Change top_up_flag to 0/1  
label_stringIdx = StringIndexer(inputCol = __, outputCol = 'label')
stages += [label_stringIdx]

numericCols = [__']
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features", handleInvalid="keep")

stages += [assembler]

cols2 = data.columns

pipeline_data = Pipeline(stages = stages)
pipelineData = pipeline_data.fit(data)
data = pipelineData.transform(data)
selectedCols = ['label', 'features'] + cols2
data = data.select(selectedCols)

scale=StandardScaler(inputCol='features',outputCol='standardized')
data_scale=scale.fit(data)
data_scale_output2=data_scale.transform(data)

predictions = gbtModel.transform(data_scale)
predictions.select(__).show(10)

#Get Data without label and features column in model
data_pred = predictions.drop("label").drop("features")

#Extract Struct Data Probability
slicer = VectorSlicer(inputCol="", outputCol="", indices=[1])
get_probs = slicer.transform(data_pred)
data_fix = get_probs.select(__)

#Change Struct Data Type to String
data_fix = data_fix.withColumn("", data_fix[""].cast("string"))

#Remove Brackets in Probability Column
data_fix = data_fix.withColumn("", F.regexp_replace("", "[\[\]]", ""))
 
#Change String to Float 
data_fix = data_fix.withColumn("", data_fix[""].cast("float"))
data_fix.show(2)

cust = spark.sql("""
__""")

df_merged = data_fix.join(cust, [''], 'inner')
df_merged.printSchema()

df1 = df_merged.withColumn("", 
          when(())
