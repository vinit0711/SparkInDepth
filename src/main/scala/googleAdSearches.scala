import org.apache.spark.{SparkConf, SparkContext}

object googleAdSearches extends App {
  val appName: String = "googleAdsSearches" //The name of Spark application on WebUI
  val master: String = "local[*]" // Spark, Mesos or YARN cluster URL
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val sc = new SparkContext(conf)
  //  code
  val input = sc.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/bigDataCampaign.csv")
  val flattenrdd = input.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))
  flattenrdd.collect.foreach(println)
//  val trimrdd=flattenrdd.flatMapValues(x=>x.split(" "))
//  val rdd3=trimrdd.map(x=>(x._2,x._1))
//  //  val wordsLower=rdd2.map(x=>x.toLowerCase())
//  //  val rdd3 = wordsLower.map(x => (x, 1))
//    val rdd4 = rdd3.reduceByKey((x, y) => x + y)
// c
}
