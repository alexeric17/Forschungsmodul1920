import Util._
import Util.spark.implicits._
import org.apache.spark.graphx.VertexId

import scala.collection.mutable.ListBuffer

object ComputePagerankCore {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()
    var result = ListBuffer[(VertexId, VertexId, List[VertexId])]()

    val pageranks = spark.read.json(pagerankFile)

    val core_nodes = pageranks
      .distinct
      .as[(VertexId, Double)]
      .collect()
      .toMap

    val core_node_ids = core_nodes.keys.toList.sortBy(v => -v)

    var iteration = 0
    core_node_ids.foreach(v => {
      iteration += 1
      val start = System.nanoTime()
      val paths = shortest_path_pregel(filtered_graph, v.toInt)
      println(s"Done computing after ${(System.nanoTime() - start) / 1000 / 1000} ms")
      paths
        .filter(v => core_node_ids.contains(v._1) && v._2._2.nonEmpty)
        .foreach(v => result.append((v._2._2.head, v._2._2.last, v._2._2)))

      if (iteration == 100 || (iteration > 100 && iteration % 10 == 0)) {
        println(s"Percentage of found paths between core after $iteration iterations: ${result.length.toDouble / 10000}")
        result.filter(t => core_node_ids.take(iteration).contains(t._1) && core_node_ids.take(iteration).contains(t._2))
          .toDF("src", "dst", "path").coalesce(1).write.json(dataDir + s"/core_degrees/$iteration")
      }
    })
  }
}
