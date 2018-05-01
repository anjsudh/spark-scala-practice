import java.nio.charset.CodingErrorAction
import java.nio.charset.CodingErrorAction.REPLACE

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}

object MostPopularMovie {
  def loadMovieNames () : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(REPLACE)
    codec.onUnmappableCharacter(REPLACE)

    val lines = Source.fromFile("src/resources/ml-100k/u.item")
      .getLines()
    var map: Map[Int, String] = Map()
    for(line <- lines) {
      val words = line.split('|')
      map += (words(0).toInt -> words(1))
    }
    return map;
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc = new SparkContext("local[*]", "RatingsCounter")
    val movies = sc.broadcast(loadMovieNames)
    val sortedResults = sc.textFile("src/resources/ml-100k/u.data")
      .map(x=>{
      val data = x.toString().split("\t");
      (data(1).toInt, 1)
    })
      .reduceByKey((x,y) => x+y)
      .map(x => (x._2, x._1))
      .sortByKey(true)
      .map(x => (movies.value(x._2), x._1))
      .collect();
    sortedResults.foreach(println);
  }
}