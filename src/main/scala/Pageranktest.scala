import Util._

object Pageranktest {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graphframe()
    val start = System.nanoTime()
    val ranks = filtered_graph.pageRank.maxIter(1).run()
    val runtime = (System.nanoTime() - start) / 1000 / 1000
    print(s"Runtime: $runtime ms")
  }
}
