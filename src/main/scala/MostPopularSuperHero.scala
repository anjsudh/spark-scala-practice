import org.apache.log4j._
import org.apache.spark._

object MostPopularSuperHero {
  def parseSuperHero(line:String ): Option[(Int, String)] = {
    val index = line.indexOf(' ')
    if (index > 0) {
      return Some(line.substring(0, index).toInt -> line.substring(index + 1))
    } else {
      return None
    };
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc = new SparkContext("local[*]", "RatingsCounter")

    val superHeroes = sc.textFile("src/resources/Marvel-names.txt")
      .flatMap(parseSuperHero)
    val famouseOne = sc.textFile("src/resources/Marvel-graph.txt")
      .map(x=>{
      val data = x.toString().split(" ");
      (data(0).toInt, data.size)
    }).reduceByKey( (x,y) => x + y )
      .map(x => (x._2, x._1))
        .max()
    println(s"${superHeroes.lookup(famouseOne._2)(0)} is the most famous fello :P")
  }
}