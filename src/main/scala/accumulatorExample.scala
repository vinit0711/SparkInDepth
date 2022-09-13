import org.apache.spark.{SparkConf, SparkContext}


object accumulatorExample extends App{
  val appName: String = "googleAdsSearches" //The name of Spark application on WebUI
  val master: String = "local[*]" // Spark, Mesos or YARN cluster URL
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val sc = new SparkContext(conf)
  //  code
  val input = sc.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/bigDataCampaign.csv")

//  initialize Accumulator

  val myaccum=sc.longAccumulator("blank lines accumulator")

  val result= input.foreach(x => if (x=="")myaccum.add(1))

  myaccum.value
}
