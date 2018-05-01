import org.apache.log4j._
import org.apache.spark._

object Wordcounting {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Word Counting")
    val lines = sc.textFile("src/resources/book.txt")
    val words = lines
      .flatMap(x => {
        x.split("\\W+")
      })
      .map(x => x.toLowerCase())
      .map(x => (x, 1.toFloat)).reduceByKey((x, y) => x + y)
      .map(x=>(x._2, x._1))
      .sortByKey(false)
      .collect()
      .foreach(x=>println(s"${x._2} : ${x._1}"))
  }
}