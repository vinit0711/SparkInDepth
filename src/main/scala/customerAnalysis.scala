import org.apache.spark.{SparkConf, SparkContext}


object customerAnalysis extends App {
  val appName: String = "customerAnalysis" //The name of Spark application on WebUI
  val master: String = "local[*]" // Spark, Mesos or YARN cluster URL
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val sc = new SparkContext(conf)
  //  code
  val input = sc.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/customerorders.csv")
  val mappedInput= input.map(x=>(x.split(",")(0),x.split(",")(2).toFloat))
  val CustomerGroupBy=mappedInput.reduceByKey(_+_)
//  val sortedCustomers=CustomerGroupBy.sortBy(x=>x._2)
//  sortedCustomers.collect.foreach(println)
  val premiumCustomers=CustomerGroupBy.filter(x=>x._2>5000)
  val doubleAmount =premiumCustomers.map(x=>(x._1, x._2 * 2))
  doubleAmount.collect.foreach(println)

//  sortedCustomers.saveAsTextFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark_output_1")
  scala.io.StdIn.readLine()

}
