import Util._

object RuntimeTest {
  def main(args: Array[String]): Unit = {
    val graph_5k = get_subgraph(nodesDF, edgeDF, 5000, take_highest = true)
    val components_5k = subgraphs_from_connected_components(graph_5k)
    val subgraphs_5k = create_all_subgraphs_from_cc(graph, components_5k, components_5k.length)
    println(s"5K: #Components: ${subgraphs_5k.length}")

    //    val graph_10k = get_subgraph(nodesDF, edgeDF, 10000, take_highest = true)
//    val graph_25k = get_subgraph(nodesDF, edgeDF, 25000, take_highest = true)
//    val graph_50k = get_subgraph(nodesDF, edgeDF, 50000, take_highest = true)

//    val source_id = 9945
//
//    //Subgraph with 5K nodes
//    var start = System.nanoTime()
//    shortest_path_graphx(graph_5k, List(source_id), source_id)
//    var end = System.nanoTime()
//    println(s"5K, graphx: ${(end - start) / 1000 / 1000} ms")
//
//    start = System.nanoTime()
//    shortest_path_pregel(graph_5k, source_id)
//    end = System.nanoTime()
//    println(s"5K, pregel: ${(end - start) / 1000 / 1000} ms")

//    //Subgraph with 10K nodes
//    start = System.nanoTime()
//    shortest_path_graphx(graph_10k, List(source_id), source_id)
//    end = System.nanoTime()
//    println(s"10K, graphx: ${(end - start) / 1000 / 1000} ms")
//
//    start = System.nanoTime()
//    shortest_path_pregel(graph_10k, source_id)
//    end = System.nanoTime()
//    println(s"10K, pregel: ${(end - start) / 1000 / 1000} ms")
//
//    //Subgraph with 25K nodes
//    start = System.nanoTime()
//    shortest_path_graphx(graph_25k, List(source_id), source_id)
//    end = System.nanoTime()
//    println(s"25k, graphx: ${(end - start) / 1000 / 1000} ms")
//
//    start = System.nanoTime()
//    shortest_path_pregel(graph_25k, source_id)
//    end = System.nanoTime()
//    println(s"25K, pregel: ${(end - start) / 1000 / 1000} ms")
//
//    //2 Subgraph with 50K nodes
//    start = System.nanoTime()
//    shortest_path_graphx(graph_50k, List(source_id), source_id)
//    end = System.nanoTime()
//    println(s"50K, graphx: ${(end - start) / 1000 / 1000} ms")
//
//    start = System.nanoTime()
//    shortest_path_pregel(graph_50k, source_id)
//    end = System.nanoTime()
//    println(s"50K, pregel: ${(end - start) / 1000 / 1000} ms")
//
//    //3 Full graph
//    start = System.nanoTime()
//    shortest_path_graphx(graph, List(source_id), source_id)
//    end = System.nanoTime()
//    println(s"full, graphx: ${(end - start) / 1000 / 1000} ms")
//
//    start = System.nanoTime()
//    shortest_path_pregel(graph, source_id)
//    end = System.nanoTime()
//    println(s"full, pregel: ${(end - start) / 1000 / 1000} ms")
  }
}
