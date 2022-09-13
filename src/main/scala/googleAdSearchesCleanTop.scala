import scala.io.Source
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

//Question- Google ad searches words all and rupess spend , filter out boring words if present

object googleAdSearchesCleanTop {

  def loadBoringWords():Set[String]={
    var boringWords:Set[String]=Set()
    val lines =Source.fromFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/boringWords.txt").getLines()
    for (line <- lines){
      boringWords+=line
    }
     boringWords
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val appName: String = "googleAdsSearchesWithoutBoringWords" //The name of Spark application on WebUI
  val master: String = "local[*]" // Spark, Mesos or YARN cluster URL
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val sc = new SparkContext(conf)
//  Here we are broadcasting the varibles to all cluster
  var boringWordsSet=sc.broadcast(loadBoringWords)
  val input = sc.textFile("/home/infinity/Documents/Spark/trendy/trendy_course_material/dataset/bigDataCampaign.csv")
  val mappedInput = input.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))

  val words = mappedInput.flatMapValues(x => x.split(" "))
  val finalMapped  = words.map(x => (x._2, x._1))
  val filterInput=finalMapped.filter(x=> ! boringWordsSet.value(x._1))
  val rdd4 = filterInput.reduceByKey((x, y) => x + y)
  rdd4.collect.foreach(println)

}
