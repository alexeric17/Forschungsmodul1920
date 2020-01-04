import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object ConvertBWToDF {

  //set this to the home directory of this project
  val FM1920HOME = ""

  def main(args: Array[String]): Unit = {
    //1. Initialize
    val conf = new SparkConf().setAppName("ConvertBluewordsToGraphDataframe").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("ConvertBluewordsToGraphDataframe")
      .enableHiveSupport()
      .getOrCreate()

    //2. Read data which was created by bluewordsextraction-project - MAKE SURE THAT THE FILE EXISTS
    val bluewordsDF = spark.read.json(FM1920HOME + "/data/bluewords.json")

    //3. Create node dataframe by dropping and renaming some columns
    val nodeDF = bluewordsDF
      .drop("Blue Words")
      .drop("#BW")
      .withColumn("id", bluewordsDF("id").cast("int"))

    //4. Create map which maps titles to their respective ids
    val titleToId = nodeDF
      .rdd
      .map(e => (e.getString(1).toLowerCase, e.getInt(0)))
      .collectAsMap()


    //5. Create Edge Dataframe by translating every blueword to its matching id
    val explodedBluewordsDF = bluewordsDF
      .drop("#BW")
      .drop("title")
      .withColumn("id", bluewordsDF("id").cast("int"))
      .withColumnRenamed("id", "src")
      .withColumn("blueword", explode(bluewordsDF("Blue Words")))
      .drop("Blue Words")

    //UDF to remove the brackets around each blueword before finding a suitable id in the titleToId-map
    val convertBWToIdUdf = udf((s: String) => titleToId.get(s.substring(2, s.length - 2)))

    val edgeDF = explodedBluewordsDF
      .withColumn("id2", convertBWToIdUdf(explodedBluewordsDF("blueword")))
      .drop("blueword")
      .filter("id2 is not null")
      .withColumnRenamed("id2", "dst")

    //6. Save the results to data-directory
    nodeDF.coalesce(1).write.json(FM1920HOME + "/data/nodes")
    edgeDF.coalesce(1).write.json(FM1920HOME + "/data/edges")
  }
}
