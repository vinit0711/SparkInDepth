import org.apache.spark.{SparkConf, SparkContext}

//Questtion
//1.Find all the movies with rating greater than 4 and minimum 10 people sould have given the rating
object topMovies extends App {
  val appName: String = "course ranking " //The name of Spark application on WebUI
  val master: String = "local[*]"
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val sc = new SparkContext(conf)
  //  code
  val ratingDataRDD = sc.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api/ratings.dat").map(x=>{
    val splittedFields=x.split("::")
    (splittedFields(1),splittedFields(2))
  })

  val moviesRdd= sc.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark-api/movies.dat").map(x => {
    val splittedFields = x.split("::")
    (splittedFields(0), splittedFields(1))
  })

  val pairredrdd=ratingDataRDD.map(x=>(x._1,(x._2.toFloat,1.0)))

  val reducedRdd=pairredrdd.reduceByKey((x,y)=>(x._1 + y._1,x._2 + y._2))

  val filterrdd=reducedRdd.filter(x=>x._2._2 > 50)

  val ratingFilter=filterrdd.mapValues(x=>x._1/x._2).filter(x=>x._2 >4.0)

  val moviesWithNamesRdd=ratingFilter.join(moviesRdd)

  moviesWithNamesRdd.collect.foreach(println)


}
