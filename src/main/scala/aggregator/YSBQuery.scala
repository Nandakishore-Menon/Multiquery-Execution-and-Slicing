package aggregator

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class YSBQuery(q_id: Int, w:Int, s:Int, slice:Int, spark: SparkSession) {

  val wind = "%d seconds".format(slice);
  val slide = "%d seconds".format(slice);


  def group_by_slice(df: DataFrame): DataFrame = { // Grouping slices to respective windows using the start time of each slice.

    val wind = "%d seconds".format(w); // set window size
    val slide = "%d seconds".format(s); // set slide length
    val grouped = df.groupBy(window(df("start"), wind, slide), df("event_type")).sum();
    return grouped;

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

    return group_by_slice(df);
  }

  def q_id(): Int = {
    return q_id;
  }
}
