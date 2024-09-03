import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q40_customer_feedback_analysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb40").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Feedback_Id","Customer_Id","Feedback_Score","Feedback_Date")

    val  customer_feedback = Seq((1,1,95,"2025-03-01"),(2,2,80,"2025-03-05"),(3,3,65,"2025-03-10"),
                                 (4,4,85,"2025-03-15"),(5,5,90,"2025-03-20"),(6,6,75,"2025-03-25"))
        val df = customer_feedback.toDF(schema:_*)
              df.show()
    /*  #Create a new column feedback_rating based on feedback_score:
   'Excellent' if feedback_score >= 90
   'Good' if 70 >= feedback_score < 90
   'Needs Improvement' if feedback_score < 70
   ====================================================*/
    val feedback_rating = df.withColumn("Feedback_Rating",
                              when($"Feedback_Score" >= 90,"Excellent")
                                .when($"Feedback_Score" >= 70 && $"Feedback_Score" < 90,"Good")
                                .when($"Feedback_Score" < 70 ,"Needs Improvement"))
       feedback_rating.show()

/* # Filter feedback where feedback_date is in 'March 2025'.
========================================================== */
val df1 = df.filter($"Feedback_Date".between("2025-03-01","2025-03-31"))
    df1.show()

/* #Calculate the total (count) of each feedback_rating.
=======================================================================*/
    val df2 = feedback_rating.groupBy("Feedback_Rating").agg(count($"Feedback_Id").alias("Total Number of Ratings"))
       df2.show()

    spark.stop() 
  }
}