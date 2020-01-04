import org.apache.spark.sql.DataFrame

object Util {

  //set this to the home directory of this project
  val FM1920HOME = ""

  def create_dict_from_nodes(nodes: DataFrame): collection.Map[Long, String] = {
    nodes
      .rdd
      .map(entry => (entry.getLong(0), entry.getString(1)))
      .collectAsMap()
  }
}
