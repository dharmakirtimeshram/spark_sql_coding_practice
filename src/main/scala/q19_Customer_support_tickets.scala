import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q19_Customer_support_tickets {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb19").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val support_ticket = List((1,"Bug",1.5,"High"),(2,"Feature",3.0,"Medium"),(3,"Bug",4.5,"Low"),
                               (4,"Bug",2.0,"High"),(5,"Enhancement",1.0,"Medium"),(6,"Bug",5.0,"Low"))
                               .toDF("Ticket_Id","Issue_Type","Resolution_Time","Priority")
    support_ticket.show()

    /*#Create a new column resolution_status based on resolution_time:
  'Quick' if resolution_time <= 2 hours
  'Moderate' if 2 > resolution_time <= 4 hours
  'Slow' if resolution_time > 4 hours
  ============================================*/
    val resolution_status =support_ticket.withColumn("Resolution_Status",
                                          when($"Resolution_Time" <= 2,"Quick")
                                            .when($"Resolution_Time" > 2 && $"Resolution_Time" <= 4,"Moderate")
                                          .when($"Resolution_Time" > 4,"Slow"))
    resolution_status.show()

/*#Filter tickets where issue_type contains 'Bug'.
 =================================================*/
    val filt = support_ticket.filter(($"Issue_Type").contains("Bug"))
    filt.show()

/*#Calculate the total (sum), average (avg), maximum (max), and minimum (min) resolution_time for each resolution_status.
=========================================================================================================================*/
    val calc = resolution_status.groupBy("Resolution_Status")
                                       .agg(sum($"Resolution_Time").alias("Total Resolution Time"),
                                         avg($"Resolution_Time").alias("Average Resolution Time"),
                                         max ($"Resolution_Time").alias("Maximum Resolution Time"),
                                         min($"Resolution_Time").alias("Minimum Resolution Time"))
    calc.show()

   spark.stop()
  }
}
