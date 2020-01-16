import Util._

object RuntimeTest {
  def main(args: Array[String]): Unit = {
    val graph_10k = get_subgraph(nodesDF, edgeDF, 10000, take_highest = true)
    val graph_50k = get_subgraph(nodesDF, edgeDF, 50000, take_highest = true)

    //1 Subgraph with 10K nodes
    var start = System.nanoTime()
    shortest_path_graphx(graph_10k, List(1), 1)
    var end = System.nanoTime()
    println(s"10K, graphx: ${(end - start) / 1000 / 1000} ms")

    start = System.nanoTime()
    shortest_path_pregel(graph_10k, 1)
    end = System.nanoTime()
    println(s"10K, pregel: ${(end - start) / 1000 / 1000} ms")

    //2 Subgraph with 50K nodes
    start = System.nanoTime()
    shortest_path_graphx(graph_50k, List(1), 1)
    end = System.nanoTime()
    println(s"50K, graphx: ${(end - start) / 1000 / 1000} ms")

    start = System.nanoTime()
    shortest_path_pregel(graph_50k, 1)
    end = System.nanoTime()
    println(s"50K, pregel: ${(end - start) / 1000 / 1000} ms")

    //3 Full graph
    start = System.nanoTime()
    shortest_path_graphx(graph, List(1), 1)
    end = System.nanoTime()
    println(s"full, graphx: ${(end - start) / 1000 / 1000} ms")

    start = System.nanoTime()
    shortest_path_pregel(graph, 1)
    end = System.nanoTime()
    println(s"full, pregel: ${(end - start) / 1000 / 1000} ms")
  }
}
