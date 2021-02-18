//package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
// $example off$

object KMeansExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
    //val data = sc.textFile("data/mllib/kmeans_data.txt")
    //val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    //PARA CORRER ESTE SCRIPT ENTRE A SPARK-SHELL Y DENTRO DE SCALA TIPEE :load nombre.scala CON ESTO SE CORRERA EL SCRIPT
    //leer archivo
    val bdjuego = sc.textFile("dato.csv")
    //quitar cabecera
    val bd = bdjuego.mapPartitionsWithIndex{(idx,iter)=>if(idx==0) iter.drop(1) else iter}

    val bdcluster = bd.map(s=>Vectors.dense(
        s.split(",")(3).toDouble,
        s.split(",")(4).toDouble,
        s.split(",")(5).toDouble,
        s.split(",")(6).toDouble,
        s.split(",")(7).toDouble
        )).cache()
    
    //val bdcluster = consulta.map(s => Vectors.dense(s.split(",").map(_.toDouble))).cache()


    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(bdcluster, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(bdcluster)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Save and load model
    clusters.save(sc, "KMeansModel")
    val sameModel = KMeansModel.load(sc, "KMeansModel")
    // $example off$

    sc.stop()
  }
}