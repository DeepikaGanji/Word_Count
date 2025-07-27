import org.apache.spark.{SparkConf, SparkContext}

object Word_Count {
  def main(args: Array[String]) {
    val master = args(0)
    val inputfile = args(1)
    val conf = new SparkConf().setAppName("Spark RDD Wordcount Example").setMaster(master)
    val sc = new SparkContext(conf)
    val lines = sc.textFile(inputfile).cache()
    val nonNullLines = lines.filter(line => line.length>0)
    val words = nonNullLines.flatMap(line => line.split(" ") )
    val upperWords = words.map(word => word.toUpperCase)
    val pairedOnes = upperWords.map(uw => (uw, 1))
    val wordCounts = pairedOnes.reduceByKey(_ + _)
    wordCounts.take(5).foreach(println)
    println("********************")
    sc.stop()  }
}
