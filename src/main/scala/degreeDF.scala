import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object degreeDF {

  //set this to the home directory of this project
  val FM1920HOME = ""

  def main(args: Array[String]): Unit = {

    //1. Initialize
    val conf = new SparkConf().setAppName("degreeDF").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("degreeDF")
      .enableHiveSupport()
      .getOrCreate()

    //2. Read edgeDF from file. Set fileNameEdgeDF to edgeDF json file.
    //  Open edgeDF. |id1|id2|. id1 -> id2

    //--For testing--
    //val fileNameEdgeDF= "data/edges/edge.json"
    //val edgeDF = spark.createDataFrame(sc.parallelize(Array((1,2),(1,3),(2,3)))).toDF("id1","id2")//spark.read.json(fileNameEdgeDF)
    //--End testing--

    val fileNameEdgeDF = "data/edges/edge.json"
    val edgeDF = spark.read.json(fileNameEdgeDF)

    //3. Create DataFrame for outEdges and inEdges
    //   OutEdges |ID|#outEdges|
    //   InEdges  |ID|#inEdges |
    val outEdges = edgeDF.select("src").groupBy("src").count()
      .withColumnRenamed("count", "outEdges").withColumnRenamed("src", "id")
    val inEdges = edgeDF.select("dst").groupBy("dst").count()
      .withColumnRenamed("count", "inEdges").withColumnRenamed("dst", "id")

    //4. Create a degreeDF with |ID|#inEdges|#outEdges|#totalEdge|
    //   Handle null values by setting them to 0. totalEdges = inEdges + outEdges
    val degreeDF = outEdges.join(inEdges, Seq("id"), joinType = "outer")
      .na.fill(0.0, Seq("outEdges")) //Set null values to 0
      .na.fill(0.0, Seq("inEdges"))
      .withColumn("totalEdges", col("outEdges") + col("inEdges")).toDF()

    //5. Save dataFrame in data-directory
    degreeDF.coalesce(1).write.json(FM1920HOME + "/data/degree")
  }
}
