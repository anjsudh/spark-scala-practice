import java.lang.System.currentTimeMillis

import org.apache.log4j._
import org.apache.spark._

object Wordcounting {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Word Counting")
    val lines = sc.textFile("src/resources/book.txt")
    val words = lines
      .flatMap(x => {x.split(" ")})
      .map(x=> x.toUpperCase())
    words.foreach(x => println(x))
    words.countByValue().foreach(println)
  }
}