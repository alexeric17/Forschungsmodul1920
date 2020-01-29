import Util._

object Pageranktest {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    val ranks = get_filtered_graphframe().pageRank.maxIter(1).run()
    val runtime = (System.nanoTime() - start) / 1000 / 1000
    print(s"Runtime: $runtime ms")
  }
}
