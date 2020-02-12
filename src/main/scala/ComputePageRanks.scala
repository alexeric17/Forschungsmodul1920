import Util._

object ComputePageRanks {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()
    val start = System.nanoTime()
    val ranks = filtered_graph.pageRank(0.000001)
    val runtime = (System.nanoTime() - start) / 1000 / 1000
    print(s"Runtime: $runtime ms\n")

    spark.createDataFrame(ranks.vertices.distinct()).toDF("id", "pagerank")
      .distinct()
      .coalesce(1)
      .write
      .json(pagerankDir)
  }
}
