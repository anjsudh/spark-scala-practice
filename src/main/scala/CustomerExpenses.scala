import org.apache.log4j._
import org.apache.spark._

object CustomerExpenses {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Customer Expenses")
    val customerData = sc.textFile("src/resources/customer-data.csv")
      .map(x => {
        val data = x.split(",")
        (data(0), data(2).toFloat)
      })
      .reduceByKey((x, y) => x + y)
      .map(x=>(x._2, x._1))
      .sortByKey(false)
      .collect()
    customerData.foreach(x=>println(s"${x._2} : ${x._1}"));
  }
}