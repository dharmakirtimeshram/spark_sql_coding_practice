import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q1_cost_feedback_analysis {
  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder().appName("qb1").master("local[4]").getOrCreate()
    import spark.implicits._

    val data = List((1,"2024-01-10",4,"Great Service"),(2,"2024-01-15",5,"Excellent"),(3,"2024-02-20",2,"Poor Experience"),
                    (4,"2024-02-25",3,"Good Value"),(5,"2024-03-05",4,"Great Quality"),(6,"2024-03-12",1,"Bad Service"),
                    (7,"2024-04-12",3,"Good Value"),(8,"2024-04-29",4,"Great Service"),(9,"2024-05-12",2, "Average"),
                    (10,"2024-05-28",5,"Excellent"),(11,"2024-06-07",2,"Average"),(12,"2024-06-16",4,"Great Service"))
                   .toDF("Customer_id","Feedback_date","Rating","Feedback_text")
  // data.show()

    /*#Create a new column rating_category based on rating:
 'Excellent' if rating >= 5
 'Good' if 3 >= rating < 5
 'Poor' if rating < 3
 ============================ */

    val rating_category = data.withColumn("Rating_category",when(($"Rating") >= 5,"Excellent")
                                                .when(($"Rating")>=3 && ($"Rating") <5,"Good")
                                                .when(($"Rating")<3,"Poor"))
                                     rating_category.show()

    /*#Filter feedback with feedback_text that starts with 'Great'.
    ================================================================== */

    val strtwith = data.filter(($"Feedback_text").startsWith("Great"))
   strtwith.show()

    /*#Calculate the average rating per month.
    ============================================ */

    val fb_month = data.withColumn("Rating_month",month($"Feedback_date"))
    val avg_rating = fb_month.groupBy("Rating_month").agg(avg(col("Rating")).alias("Average_rating"))
    avg_rating.show()

   spark.stop()
  }

}
