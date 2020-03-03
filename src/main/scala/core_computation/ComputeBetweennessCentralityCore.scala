package core_computation

import org.apache.spark.graphx.VertexId
import util.Subgraphs._
import util.Util._
import util.Util.spark.implicits._

import scala.collection.mutable.ListBuffer

object ComputeBetweennessCentralityCore {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()
    var result = ListBuffer[(VertexId, VertexId, List[VertexId])]()
    val connected_components = subgraphs_from_connected_components(filtered_graph).sortBy(c => -c.size)
    val biggest_component = connected_components(0).toSet

    val bc_values = spark.read.json(betweennessCentralityFile)

    val core_nodes = bc_values
      .distinct
      .as[(VertexId, Int)]
      .collect()
      .filter(v => biggest_component.contains(v._1))
      .sortBy(v => -v._2)
      .take(5000)

    val core_node_ids = core_nodes.map(n => n._1)

    var iteration = 0
    core_nodes.foreach(v => {
      iteration += 1
      val start = System.nanoTime()
      val paths = shortest_path_pregel(filtered_graph, v._1.toInt)
      println(s"Done computing after ${(System.nanoTime() - start) / 1000 / 1000} ms")
      paths
        .filter(p => core_node_ids.contains(p._1) && p._2._2.nonEmpty)
        .foreach(v => result.append((v._2._2.head, v._2._2.last, v._2._2)))

      if (iteration == 100 || (iteration > 100 && iteration % 10 == 0)) {
        println(s"Percentage of found paths between core after $iteration iterations: ${result.length.toDouble / 10000}")
        result
          .filter(t => core_node_ids.take(iteration).contains(t._1) && core_node_ids.take(iteration).contains(t._2))
          .toDF("src", "dst", "path")
          .coalesce(1)
          .write.json(dataDir + s"/core_betweenness_centrality/$iteration")
      }
    })
  }
}
