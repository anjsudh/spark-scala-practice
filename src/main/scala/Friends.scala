import org.apache.log4j._
import org.apache.spark._

object Friends {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Friends")
    val lines = sc.textFile("src/resources/fakeFriends.csv")
    val data = lines.map(x => {
      val arr = x.toString().split(",")
      val age = arr(2)
      val noOfFriends = arr(3).toInt
      (age, (noOfFriends, 1))
    })
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues((y) => (y._1 / y._2))
      .sortByKey(true)
      .foreach(println)

  }
}