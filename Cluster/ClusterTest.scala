import org.apache.spark.sql.types._
import spark.implicits._ 
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vectors, Vector}

val schema = StructType(
  StructField("Platform", StringType, nullable = true) ::
    StructField("Genre", StringType, nullable = true) ::
    StructField("Publisher", StringType, nullable = true) ::
    StructField("NA_Sales", DoubleType, nullable = true) ::
    StructField("EU_Sales", DoubleType, nullable = true) ::
    StructField("JP_Sales", DoubleType, nullable = true) ::
    StructField("Other_Sales", DoubleType, nullable = true) ::
    StructField("Global_Sales", DoubleType, nullable = true) ::
    StructField("Rating", StringType, nullable = true) ::    
    StructField("Critic_Score_Class", StringType, nullable = true) ::
    Nil
)

// read to DataFrame
val videogameDf = spark.read.format("csv").option("header", value = true).option("delimiter", ",").option("mode", "DROPMALFORMED").schema(schema).load("dato.csv").cache()

videogameDf.printSchema()
videogameDf.show(10)

//add feature col
val cols = Array("NA_Sales","EU_Sales","JP_Sales","Other_Sales","Global_Sales")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featureDf = assembler.transform(videogameDf)

featureDf.printSchema()
featureDf.show(10)


//dividir data en 70/30
val seed = 1984
val Array(trainingData, testData) = featureDf.randomSplit(Array(0.7, 0.3), seed)



//build kmeans 8 cluster

val kmeans = new KMeans().setK(8).setFeaturesCol("features").setPredictionCol("prediction")

val kmeansModel = kmeans.fit(trainingData)

println()
println("|-------------|OUTPUT|-------------|")
println()

kmeansModel.clusterCenters.foreach(println)

val predictDf = kmeansModel.transform(testData)

println()
println("|-------------|PREDICCIONES|-------------|")
println()

predictDf.show(10)
predictDf.groupBy("prediction").count().show()

// calculate distance from center
val distFromCenter = udf((features: Vector, c: Int) => Vectors.sqdist(features, kmeansModel.clusterCenters(c)))
val distanceDf = predictDf.withColumn("distance", distFromCenter($"features", $"prediction"))
distanceDf.show(10)


// no of categories
predictDf.groupBy("prediction").count().show()

// save model
kmeansModel.write.overwrite().save("videogame-model")

