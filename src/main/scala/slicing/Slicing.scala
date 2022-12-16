package slicing

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
//import org.apache.spark.sql.execution.ui._

// The slicing application that creates slices and writes the partial aggregates to a kafka topic
object Slicing extends App{
  val spark = SparkSession
    .builder()
    .appName("Multiquery execution")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

    val listener = new Listener()
    spark.streams.addListener(listener)

    spark.sql("SET spark.sql.streaming.metricsEnabled=true")

  val KAFKA_TOPIC_NAME = "slice";
  val KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092";
  println("First SparkContext:");
  println("APP Name :"+spark.sparkContext.appName);
  println("Deploy Mode :"+spark.sparkContext.deployMode);
  println("Master :"+spark.sparkContext.master);

  val schema = StructType(Array(
    StructField("ip_address", StringType, false),
    StructField("user_id", StringType, false),
    StructField("page_id", StringType, false),
    StructField("ad_id", StringType, false),
    StructField("event_type", StringType, false)
  ))

  val mapping = spark.read.csv("data/mapping.csv").toDF("ad_id", "c_id") //join operation for YSB. gives campaign id
  mapping.printSchema()

  val inp = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
    .option("subscribe", KAFKA_TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .load()

  val df = inp.selectExpr("CAST(value AS STRING) as json", "timestamp")
    .select(from_json($"json", schema = schema).as("data"), $"timestamp")
    .select($"data.*", $"timestamp")
    .withWatermark("timestamp", "5  seconds") // adds watermark


  val query = new YSBQuery(3, 10, 2, 1, spark);


  val q = query.runQuery(df, mapping);


  def q_save = (df: Dataset[Row], batchId: Long) => {
    df.write.format("csv")
      .option("path", "target/output/query/window%d".format(batchId))
      .save()
  }


  val q3_result = q
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "sliced_data")
    .option("checkpointLocation", "target/checkpoints")
    .start()




  spark.streams.awaitAnyTermination();
}