package slicing

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class YSBQuery(q_id: Int, w:Int, s:Int, slice:Int, spark: SparkSession) {

  val wind = "%d seconds".format(slice); // setitng window size
  val slide = "%d seconds".format(slice); // setting slide length

  // JOIN operation
  def join_map(df: DataFrame, mapping: DataFrame): DataFrame = {
    return df.join(mapping, df("ad_id") === mapping("ad_id"), "inner").drop(mapping("ad_id"));
  }
  // FILTER operation
  def filter_by_event(df: DataFrame, event_type: String): DataFrame = {
    val grouped = df.groupBy(window(df("timestamp"), wind, slide), df("event_type")).count();
    return grouped.filter(grouped("event_type") === event_type);
  }

  // Add start time of each slice to initiate aggregation in the Aggregator code.
  def group_by_slice(df: DataFrame): DataFrame = {
    val df1 = df.select(col("*"), df("window")("start").as("start")) // add start time of each slice.
    // we see that multiple aggregations are not possible. Thus, we write the intermediate result ot kafka and read them again in
    // an aggregator program

//    val wind = "%d seconds".format(w);
//    val slide = "%d seconds".format(s);
//    val grouped = df1.groupBy(window(df1("start"), wind, slide), df1("event_type")).sum();
    return df1;

    //      StructType(df.schema.fields :+ StructField("id", LongType, false)))
//      val df2 = df1.withColumn("row_index", row_number().over(Window.orderBy("start")))
//    return df1.withColumn("row_number", row_number.over(windowSpec))
//    return df1;
  }

  def runQuery(df: DataFrame, mapping: DataFrame): DataFrame = {
    var event_type = "";
    if (q_id == 1) {
      event_type = "click";
    }
    else if (q_id == 2) {
      event_type = "view";
    }
    else if (q_id == 3) {
      event_type = "purchase";
    }
    val event_filtered = filter_by_event(join_map(df, mapping), event_type);
    return group_by_slice(event_filtered);
  }

  def q_id(): Int = {
    return q_id;
  }
}
