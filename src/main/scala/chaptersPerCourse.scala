import org.apache.spark.{SparkConf, SparkContext}


// Question 1 - Find Out how many Chapters  are  there per  course

object chaptersPerCourse extends App {
  val appName: String = "chaptersPerCourse" //The name of Spark application on WebUI
  val master: String = "local[*]"
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val sc = new SparkContext(conf)
  //  code
  val input = sc.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark_in_depth_assignment/chapters.csv")
  val rdd1=input.map(x=> (x.split(",")(1).toInt,1))
  val reduceRdd=rdd1.reduceByKey(_+_)
  val sortedCourses=reduceRdd.sortBy(x=>x._1)

  sortedCourses.collect.foreach(println)



}
