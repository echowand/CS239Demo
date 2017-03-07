import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object KMeansDemo{
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("KMeansDemo")
	    val sc = new SparkContext(conf)

	    // $example on$
	    // Load and parse the data
	    val data = sc.textFile("data/mllib/kmeans_data.txt")
	    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

	    // Cluster the data into two classes using KMeans
	    val numClusters = 3
	    val numIterations = 100
	    val clusters = KMeans.train(parsedData, numClusters, numIterations)

	    // Evaluate clustering by computing Within Set Sum of Squared Errors
	    val WSSSE = clusters.computeCost(parsedData)
	    println("Within Set Sum of Squared Errors = " + WSSSE)

	    // Save and load model
	    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
	    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
	    // $example off$

	    sc.stop()
	}
}