package aggregator

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
//import org.apache.spark.sql.execution.ui._


object Aggregate extends App{ // Aggregator program that reads the partial aggregate data from kafka and performs
  // the final aggregation over overlapping windows.
  val spark = SparkSession
    .builder()
    .appName("Aggregator")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

    val listener = new Listener()
    spark.streams.addListener(listener)

    spark.sql("SET spark.sql.streaming.metricsEnabled=true")

  val KAFKA_TOPIC_NAME = "sliced_data";
  val KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092";
  println("First SparkContext:");
  println("APP Name :"+spark.sparkContext.appName);
  println("Deploy Mode :"+spark.sparkContext.deployMode);
  println("Master :"+spark.sparkContext.master);

  val schema = StructType(Array(
    StructField("event_type", StringType, false),
    StructField("count", IntegerType, false),
    StructField("start", TimestampType, false),
    StructField("window", StructType(Array(StructField("start", TimestampType, false), StructField("end", TimestampType, false))), false)
  ))

  val mapping = spark.read.csv("data/mapping.csv").toDF("ad_id", "c_id")
  mapping.printSchema()

  val inp = spark
    .readStream
    .format("kafka") // read the partial aggregates form kafka
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
    .option("subscribe", KAFKA_TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .load()

  val df = inp.selectExpr("CAST(value AS STRING) as json")
    .select(from_json($"json", schema = schema).as("data"))
    .select($"data.*")
    .withWatermark("start", "10  seconds")


  val query3 = new YSBQuery(3, 10, 2, 1, spark);


  val q3 = query3.runQuery(df, mapping);


val q3_result = q3
  .writeStream
  .format("console") // print the final aggregate to the console.
  .option("truncate", "false")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .outputMode("append")
  .start()


  spark.streams.awaitAnyTermination();
}