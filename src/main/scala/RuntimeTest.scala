import Util._
import org.apache.spark.graphx.Graph

object RuntimeTest {

  def compute_runtime(size: String, components: Array[Graph[String, Double]]): Unit = {
    println(s"$size: #Components: ${components.length}")
    val sorted_components = components.sortBy(g => -g.vertices.collect().length)
    var runtime_graphx = 0.0
    var runtime_pregel = 0.0
    for (i <- 0 until 10) {
      val component = sorted_components(i)
      println(s"Size of component $i: ${component.vertices.collect().length}")
      val source_id = component.vertices.collect().take(1)(0)._1
      var start = System.nanoTime()
      shortest_path_graphx(component, List(source_id), source_id.toInt)
      var runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to graphx")
      runtime_graphx += runtime.toFloat

      start = System.nanoTime()
      shortest_path_pregel(component, source_id.toInt)
      runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " to pregel")
      runtime_pregel += runtime.toFloat
    }
    val avg_runtime_graphx = runtime_graphx / 10
    val avg_runtime_pregel = runtime_pregel / 10
    println(s"$size, graphx: $avg_runtime_graphx ms")
    println(s"$size, pregel: $avg_runtime_pregel ms")
  }

  def main(args: Array[String]): Unit = {
    val graph_5k = get_subgraph(nodesDF, edgeDF, 5000, take_highest = true)
    val components_5k = subgraphs_from_connected_components(graph_5k)
    val subgraphs_5k = create_all_subgraphs_from_cc(graph_5k, components_5k, components_5k.length)

    val graph_10k = get_subgraph(nodesDF, edgeDF, 10000, take_highest = true)
    val components_10k = subgraphs_from_connected_components(graph_10k)
    val subgraphs_10k = create_all_subgraphs_from_cc(graph_10k, components_10k, components_10k.length)

    val graph_25k = get_subgraph(nodesDF, edgeDF, 25000, take_highest = true)
    val components_25k = subgraphs_from_connected_components(graph_25k)
    val subgraphs_25k = create_all_subgraphs_from_cc(graph_25k, components_25k, components_25k.length)

    val graph_50k = get_subgraph(nodesDF, edgeDF, 50000, take_highest = true)
    val components_50k = subgraphs_from_connected_components(graph_50k)
    val subgraphs_50k = create_all_subgraphs_from_cc(graph_50k, components_50k, components_50k.length)

    val components_full = subgraphs_from_connected_components(graph)
    val subgraphs_full = create_all_subgraphs_from_cc(graph, components_full, components_full.length)

    compute_runtime("5K", subgraphs_5k)
    compute_runtime("10K", subgraphs_10k)
    compute_runtime("25K", subgraphs_25k)
    compute_runtime("50K", subgraphs_50k)
    compute_runtime("FULL", subgraphs_full)
  }
}
