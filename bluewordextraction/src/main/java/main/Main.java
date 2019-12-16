package main;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Locale;
import org.apache.spark.SparkConf;
import java.lang.Cloneable;
//import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.mutable.WrappedArray;

public class Main {

	// zur Anzeige der benötigten Zeit pro Operation
	private static LocalDateTime start = LocalDateTime.now();

	public static void main(String[] args) {

		//Festlegen der maxResultSize - überschreiben der Standard-1Gb
		SparkConf conf = new SparkConf()
				.set("spark.files.overwrite", "true").set("spark.driver.maxResultSize","8g"); // 0 to remove the bottleneck - infi$

		//Starten der Spark Session und Anwenden der Config
		SparkSession spark = SparkSession.builder().appName("Main").master("local[*]")
				//.set("spark.driver.maxResultSize","4000")
				.config(conf).getOrCreate();

		//.set("spark.files.overwrite", "true").set("spark.driver.maxResultSize","4000.0 MB"); // 0 to remove the bottleneck - infinite

		//spark.set("spark.driver.maxResultSize","4000");

		//val conf =new SparkConf().set("spark.driver.maxResultSize","4g"));
		//val sc = new SparkContent(conf)
		//val sc = new SparkContext(new SparkConf());  //um maxResultSize beim Compiliervorgang festlegen zu können mit --conf spark.driver.maxResultSize="4000"
		spark.sparkContext().setLogLevel("ERROR");
		//spark.driver.maxResultSize='4G';
		//spark.config(conf);
		printSparkBegin();

		SQLContext context = new org.apache.spark.sql.SQLContext(spark);

		// -------------------------------------------------------------------------------
		// CSV Schema

		StructType schema = new StructType(new StructField[] {
				new StructField("title", DataTypes.StringType, false, Metadata.empty()),
				new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("date", DataTypes.StringType, false, Metadata.empty()),
                new StructField("article", DataTypes.StringType, false, Metadata.empty()),
        });

        // -------------------------------------------------------------------------------
        // df: Einlesen der wiki_9.csv
        Dataset<Row> df2 = spark.read().json("/scratch/Forschungsmodul1920/simplewiki_fulltext.json");

        // df.write().format("csv").option("delimiter", "\t").save(pathToFolder
        // +"wiki_9_20000_folder.csv");

        //showNSchema(df2, "wiki_9.csv as table: csv dropped null", 200, true);

        Dataset<Row> df3 = df2.select(df2.col("id"), df2.col("title"), org.apache.spark.sql.functions.explode(df2.col("revision")).as("article"));

        Dataset<Row> df = df3.select(df3.col("title"), df3.col("id"), df3.col("article.timestamp").as("date"), df3.col("article.text").as("article"));
        df = df.withColumn("id", df.col("id").cast("Int"));

        // Rauslöschen der Zeilen, die zwar dem Schema entsprechen, aber leere 'Strings' enthalten --> schlechtes Null-Handling
        df = df.na().drop();

        showNSchema(df, "wiki_9.csv as table: csv dropped null", 200, true);


        // -------------------------------------------------------------------------------
		// rtGetWords: Tokenize 1 - Count der Wörter in den Revisionen in rtdAllWords
		RegexTokenizer rtGetWords = new RegexTokenizer()
				.setInputCol("article")
				.setOutputCol("words")
				.setPattern("\\W");

		spark.udf().register("countTokens", (WrappedArray<?> words) -> words.size(),
				DataTypes.IntegerType);

		Dataset<Row> rtdAllWords = rtGetWords.transform(df);

		showNSchema(rtdAllWords.select("id", "article", "words")
						.withColumn("tokens", callUDF("countTokens", col("words")))
						.sort(org.apache.spark.sql.functions.desc("words")),
				"rtdAllWords: count of words per revision", 0, true);
		String pattern1 = "((?:\\x5b\\x5b)([a-zA-Z\\s|=:]*)(?:\\x5d\\x5d))*";

		RegexTokenizer rtGetBlueWords = new RegexTokenizer()
				.setInputCol("article")
				.setOutputCol("Blue Words")
				.setGaps(false)
				.setPattern(pattern1)
				.setToLowercase(true);

		Dataset<Row> rtdBlueWords = rtGetBlueWords.transform(df);

		Dataset<Row> count_BW_per_rev = rtdBlueWords;
		//showNSchema(count_BW_per_rev.select("title", "id", "date", "article", "Blue Words")
		//				.withColumn("#BW", callUDF("countTokens", col("Blue Words"))),
		//		"count_BW_per_rev: count of blue words per revision", 300, true);

		Dataset<Row> outputFile = count_BW_per_rev.select("title","id","Blue Words").withColumn("#BW",callUDF("countTokens",col("Blue Words")));
		outputFile.show(20);
		outputFile.coalesce(1).write().json("/scratch/simple_english_linkextracted");

		System.out.println("Spark finished, total elapsed time: " + elapsedTime(start, LocalDateTime.now())+ "\n\n\n\n\n\n\n\n");
		spark.stop();
	}

	public static Duration elapsedTime(LocalDateTime begin, LocalDateTime end) {
		Duration duration = Duration.between(begin, end);
		return duration;
	}

	private static int counter(String quantity) {

		return quantity.trim().split("\\s+").length;
	}

	/**
	 * Prints a label, the number of lines, a given number of rows and the
	 * schema of a given Dataset
	 *
	 * @param set
	 * @param lines
	 *            falls keine Angage gewollt dann 0 eingeben und es werden 20
	 *            Zeilen gezeigt
	 * @param truncate
	 *            Falls true werden Strings die länger als 20 chars sind
	 *            gekuerzt und rechts anghaengt
	 */
	private static void showNSchema(Dataset<Row> set, String name, int lines,
									boolean truncate) {
		DecimalFormat decf = new DecimalFormat("###,###,###");
		decf.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.GERMANY));

		LocalDateTime start = LocalDateTime.now();
		int count = (int) set.count();
		System.out.println(
				"*******************************************************************************************");
		System.out.println(name);
		System.out.println("Anzahl Zeilen: "
				+ decf.format(count));
		if (lines == 0) {
			set.show(truncate);
		} else {
			set.show(lines, truncate);
		}
		set.printSchema();
		System.out.println(
				"Elapsed Time: " + elapsedTime(start, LocalDateTime.now()));
		System.out.println(
				"*******************************************************************************************");
	}

	private static void printSparkBegin() {
		System.out.println("\n" + "Spark started..." + "\n");
	}

	private static String[] truncate(String[] stringArr) {
		int i = 0;
		String[] retArr = new String[stringArr.length];

		for (String str : stringArr) {

			if (str.charAt(0) == '['
					&& str.charAt(1) == '['
					&& str.charAt(str.length() - 1) == ']'
					&& str.charAt(str.length() - 2) == ']') {
				retArr[i++] = str.substring(2, str.length() - 2);
			}
		}
		return retArr;
	}

}
