package org.multiquery
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class YSBQuery(q_id: Int, spark: SparkSession) {
  def join_map(df: DataFrame, mapping: DataFrame): DataFrame = {
    return df.join(mapping, df("ad_id") === mapping("ad_id"), "inner").drop(mapping("ad_id"));
  }
  def filter_by_event(df: DataFrame, event_type: String): DataFrame = {
    //windowing operation. Setting the window length and slide duration. Followed by filter operation.
    val grouped = df.groupBy(window(df("timestamp"), "10 seconds", "7 seconds"), df("event_type")).count();
    return grouped.filter(grouped("event_type") === event_type).drop("window");
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
    return filter_by_event(join_map(df, mapping), event_type);
//    return join_map(df, mapping)
  }

  def q_id(): Int = {
    return q_id;
  }
}
