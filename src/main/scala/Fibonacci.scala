object Fibonacci {

  def foo(str: String ): String = {
    str.toUpperCase
  }

  def transform(str: String, foo: String => String): String = {
    foo(str);
  }

  def main(args: Array[String]): Unit = {
    var f0 = 0;
    var f1 = 1;
    println(f0);
    println(f1);
    for (i <- 3 to 10) {
      println({
        f0 + f1
      });
      val temp = f0 ;
      f0 = f1;
      f1 = temp + f1;
    }

    println(transform("blah", foo));
    println(transform("blah", str => str.toUpperCase));
  }
}
