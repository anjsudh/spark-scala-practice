import org.apache.log4j._
import org.apache.spark._

object Weather {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Weather Min Temperature")
    val lines = sc.textFile("src/resources/weather.csv")
    lines.map(x => {
      val raw = x.toString().split(",")
      (raw(0), raw(2), raw(3).toFloat)
    })
      .filter((x) => x._2.equalsIgnoreCase("TMIN"))
      .map((x) => (x._1, x._3))
      .reduceByKey((x,y) => Math.min(x,y))
      .foreach(x => println(x))
  }
}