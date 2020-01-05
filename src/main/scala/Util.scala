import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Util {

  //set this to the home directory of this project
  val FM1920HOME = ""

  def create_dict_from_nodes(nodes: DataFrame): collection.Map[Long, String] = {
    nodes
      .rdd
      .map(entry => (entry.getLong(0), entry.getString(1)))
      .collectAsMap()
  }

  def get_subgraph(nodes: DataFrame, edges: DataFrame, ids: List[Long]): Unit = {
    val timestamp = System.currentTimeMillis().toString
    nodes
      .filter(entry => ids.contains(entry.getLong(0)))
      .distinct()
      .coalesce(1)
      .write
      .json(FM1920HOME + "/data/subgraph_nodes/" + timestamp)

    edges
      .filter(entry => ids.contains(entry.getLong(0)) && ids.contains(entry.getLong(1)))
      .distinct()
      .coalesce(1)
      .write
      .json(FM1920HOME + "/data/subgraph_edges/" + timestamp)
  }

  def get_subgraph(nodes: DataFrame, edges: DataFrame, min: Int, max: Int): Unit = {
    val list = List
      .range(min, max + 1, 1)
      .map(_.toLong)

    get_subgraph(nodes, edges, list)
  }

  def get_subgraph(nodes: DataFrame, edges: DataFrame, nr: Int, take_highest: Boolean): Unit = {
    val spark = SparkSession
      .builder()
      .appName("get_subgraph")
      .enableHiveSupport()
      .getOrCreate()

    val degreeDF = spark.read.json(FM1920HOME + "/data/degree/degrees.json")

    val list = if (take_highest) {
      degreeDF
        .orderBy(desc("outEdges"))
        .select("id")
        .take(nr)
        .toList
    } else {
      degreeDF
        .orderBy(asc("outEdges"))
        .select("id")
        .take(nr)
        .toList
    }
    val string_list = list.map(r => r.toString())
    val filtered_string_list = string_list.map(s => s.substring(1, s.length - 1))
    get_subgraph(nodes, edges, filtered_string_list.map(s => s.toLong))
  }
}
