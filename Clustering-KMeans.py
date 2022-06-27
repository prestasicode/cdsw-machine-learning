from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import Imputer 
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import PCA as PCAml
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import BisectingKMeans

spark = SparkSession.builder.appName("explore").config(conf=SparkConf().setAll([('spark.yarn.queue', 'root.cdsw')])).getOrCreate()
spark.conf.set('spark.sql.caseSensitive', False)

#Read Data
data_non_indv = spark.sql("*insert query data")

#Drop Variables
data_non_indv = data_non_indv.drop('*insert data columns')
cols = data_non_indv.columns

categoricalColumns = ['*insert categorical columns']
stages = []

for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    stages += [stringIndexer, encoder]

# Change top_up_flag to 0/1  
#label_stringIdx = StringIndexer(inputCol = 'TOP_UP_FLAG', outputCol = 'label')
#stages += [label_stringIdx]

numericCols = ['*insert numerical columns']


assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features", handleInvalid="keep")

stages += [assembler]

silhouette_score=[]
evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized', \
                                metricName='silhouette', distanceMeasure='squaredEuclidean')

with open("silhouette.txt","w") as file:
  for i in range(8,15):
    KMeansalgo =KMeans(featuresCol='standardized', k=i, maxIter = 500, initSteps=50)
    KMeans_fit=KMeansalgo.fit(data_scale_output)
    output=KMeans_fit.transform(data_scale_output)
    score=evaluator.evaluate(output)
    silhouette_score.append(score)
    msg = "Number cluster :", i, "Silhouette Score:",score
    file.write(str(msg))
    
    
def output_agg(output_table_1):
    query = f"""select * from {output_table_1}"""
    print(query)
    output_agg_1 = spark.sql(query)
    return output_agg_1
