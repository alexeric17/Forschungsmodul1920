import Util._
import org.apache.spark.graphx.VertexId

object RuntimeTest {
  def main(args: Array[String]): Unit = {
    val graph_10k = get_subgraph(nodesDF, edgeDF, 10000, take_highest = true)
    val vertices_10k = graph_10k.vertices.collect().toList.map(r => r._1)

    val graph_50k = get_subgraph(nodesDF, edgeDF, 50000, take_highest = true)
    val vertices_50k = graph_50k.vertices.collect().toList.map(r => r._1)
    val vertices_full = graphFrame.vertices.select("id").collect().map(_(0)).toList.map(r => r.asInstanceOf[VertexId])

    //1 Subgraph with 10K nodes
    shortest_path_graphx(graph_10k, vertices_10k, 1)
    shortest_path_pregel(graph_10k, 1)
    //2 Subgraph with 50K nodes
    shortest_path_graphx(graph_50k, vertices_50k, 1)
    shortest_path_pregel(graph_50k, 1)
    //3 Full graph
    shortest_path_graphx(graph, vertices_full, 1)
    shortest_path_pregel(graph, 1)
  }
}
