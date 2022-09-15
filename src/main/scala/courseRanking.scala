
import org.apache.spark.{SparkConf, SparkContext}

object courseRanking extends App {
  val appName: String = "course ranking " //The name of Spark application on WebUI
  val master: String = "local[*]"
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val sc = new SparkContext(conf)
  //  code
  val input = sc.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark_in_depth_assignment/views/*.csv")
  val viewDataRDD=input.map(x=> (x.split(",")(0).toInt,x.split(",")(1).toInt))
  val chapterDataRDD=sc.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark_in_depth_assignment/chapters.csv").map (x => {
    val chapterDataFields = x.split(",")
    (chapterDataFields(0).toInt, chapterDataFields(1).toInt)

  })
  val chapterCountRDD = chapterDataRDD.map(x => (x._2,1)).reduceByKey((x,y) => x + y)

  //Create Base RDD for titles data
  val titlesDataRDD = sc.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark_in_depth_assignment/titles.csv").map(x => {
    (x.split(",")(0).toInt, x.split(",")(1))
  })

  //Step 1:Removing Duplicate Views from views RDD
  val viewDataDistinctRDD = viewDataRDD.distinct()

  //Step 2: Joining chapterDataRDD with viewDataDistinctRDD, to get CourseID also.Join key is chapterID
  //First flip the viewDataDistinctRDD to make chapterId as the key
  val flippedviewDataRDD = viewDataDistinctRDD.map(x => (x._2, x._1))

  //JOIN flippedviewDataRDD with chapterDataRDD to get the courseIDs as well
  val joinedRDD = flippedviewDataRDD.join (chapterDataRDD)

//  Now we will get rid of chapter Id as indv counting is mo use we want total so asual will do first 1 and then reduce by key
  val pairRDD=joinedRDD.map(x=>((x._2._1,x._2._2),1))
  //Step 4 - Count Views for User/Course -Finding out count of number of chapters a user has watched per course
  val userPerCourseViewRDD = pairRDD.reduceByKey(_ + _)
//  pairRDD.saveAsTextFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/spark_in_depth_assignment/views/result")

  //Step 5 Dropping the UserID going forward
  val courseViewsCountRDD = userPerCourseViewRDD.map(x =>(x._1._2,x._2))

//step 6 join with total chapter in course rdd
  val newJoinedRDD = courseViewsCountRDD.join(chapterCountRDD)
  val CourseCompletionpercentRDD = newJoinedRDD.mapValues( x => (x._1.toDouble/x._2))
  val formattedpercentageRDD = CourseCompletionpercentRDD.mapValues(x => f"$x%01.5f".toDouble)

  //Step-8 Map Percentages to Scores
  val scoresRDD = formattedpercentageRDD.mapValues(x => {
    if (x >= 0.9) 10l
    else if (x >= 0.5 && x < 0.9) 4l
    else if (x >= 0.25 && x < 0.5) 2l
    else 0l
  })

  //Step -9 Adding up the total scores for a course
  val totalScorePerCourseRDD = scoresRDD.reduceByKey((V1, V2) => V1 + V2)

  val sortedtotalScorePerCourseRDD=totalScorePerCourseRDD.sortBy(x=>x._1)

  sortedtotalScorePerCourseRDD.collect.foreach(println)

}
