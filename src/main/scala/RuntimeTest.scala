import Util._
import org.apache.spark.graphx.Graph

object RuntimeTest {

  def compute_runtime(components: Array[Graph[String, Double]]): Unit = {
    val sorted_components = components.sortBy(g => g.vertices.collect().length)
    var runtime_graphx = 0
    var runtime_pregel = 0
    for (i <- 0 until 10) {
      val component = sorted_components(i)
      val source_id = component.vertices.collect().take(1)(0)._1
      var start = System.nanoTime()
      shortest_path_graphx(component, List(source_id), source_id.toInt)
      runtime_graphx += (System.nanoTime() - start)

      start = System.nanoTime()
      shortest_path_pregel(component, source_id.toInt)
      runtime_pregel += (System.nanoTime() - start)
    }
    println(s"5K, graphx: ${runtime_graphx / 1000 / 1000} ms")
    println(s"5K, pregel: ${runtime_pregel / 1000 / 1000} ms")
  }

  def main(args: Array[String]): Unit = {
    val graph_5k = get_subgraph(nodesDF, edgeDF, 5000, take_highest = true)
    val components_5k = subgraphs_from_connected_components(graph_5k)
    val subgraphs_5k = create_all_subgraphs_from_cc(graph_5k, components_5k, components_5k.length)
    println(s"5K: #Components: ${subgraphs_5k.length}")
    println("Nodes: " + subgraphs_5k(0).vertices.collect().mkString("\n"))
    //    val graph_10k = get_subgraph(nodesDF, edgeDF, 10000, take_highest = true)
//    val graph_25k = get_subgraph(nodesDF, edgeDF, 25000, take_highest = true)
//    val graph_50k = get_subgraph(nodesDF, edgeDF, 50000, take_highest = true)

    val source_id = 9945
//
    //Subgraph with 5K nodes
    var start = System.nanoTime()
    shortest_path_graphx(graph_5k, List(source_id), source_id)
    var end = System.nanoTime()
    println(s"5K, graphx: ${(end - start) / 1000 / 1000} ms")

    start = System.nanoTime()
    shortest_path_pregel(graph_5k, source_id)
    end = System.nanoTime()
    println(s"5K, pregel: ${(end - start) / 1000 / 1000} ms")

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
