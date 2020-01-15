import Util._

object Graphxtest {
  def main(args: Array[String]): Unit = {
    val ranks = graphFrame.pageRank.maxIter(1).run().vertices

    ranks.collect().foreach(n => println(n))
  }
}
