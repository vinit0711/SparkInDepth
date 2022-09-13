import org.apache.spark.{SparkConf, SparkContext}

object loglevelAnalysis extends  App {
  val appName: String = "googleAdsSearches" //The name of Spark application on WebUI
  val master: String = "local[*]" // Spark, Mesos or YARN cluster URL
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val sc = new SparkContext(conf)

  val myList=List("WARN: Tuesday 4 sept",
  "ERROR: Tuesday 4 sept",
  "ERROR: Tuesday 4 sept",
  "ERROR: Tuesday 4 sept",
  "WARN: Tuesday 4 sept")

  val orignalRdd=sc.parallelize(myList)
  val modRdd =orignalRdd.map(x=>{
    val logLevelSplit=x.split(":")
    val logLevel=logLevelSplit(0)
    (logLevel,1)
  })
  val result=modRdd.reduceByKey(_+_)
  result.collect.foreach(println)
  scala.io.StdIn.readLine()
}


