import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q21_customer_Service_feedback_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q21").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Feedback_Id","Customer_Id","Feedback_Score","Feedback_Date")

    val customer_feedback = Seq((1,101,9,"2027-01-01"),(2,102,8,"2027-01-05"),(3,103,6,"2027-01-10"),
                                 (4,104,4,"2027-01-15"),(5,105,10,"2027-01-20"),(6,106,7,"2027-01-25"))
    val df = spark.createDataFrame(customer_feedback).toDF(schema:_*)
    df.show()

    /* Create a new column feedback_rating based on feedback_score:
          o 'Excellent' if feedback_score >= 9
          o 'Good' if 7 >= feedback_score < 9
          o 'Average' if 5 >= feedback_score < 7
          o 'Poor' if feedback_score < 5
 ==========================================================*/

    val feedback_rating = df.withColumn("Feedback_Rating",
                             when($"Feedback_Score" >= 9,"Excellent")
                               .when($"Feedback_Score" >= 7 && $"Feedback_Score" < 9,"Good")
                               .when($"Feedback_Score" >= 5 && $"Feedback_Score" < 7,"Average")
                               .when($"Feedback_Score" < 5 ,"Poor"))
    feedback_rating.show()

/*  Filter records where feedback_date is in 'January 2027'.
================================================================= */

    val df1 = df.filter($"Feedback_Date".between("2027-01-01","2027-01-31"))
    df1.show()
     
/* Calculate the total count of feedbacks, average (avg), maximum (max), and minimum (min) feedback_score for each feedback_rating.
==================================================================================================================================*/

    val df2 = feedback_rating.groupBy("Feedback_Rating")
                              .agg(sum($"Feedback_Score").alias("Total_Feedback_Score"),
                                avg($"Feedback_Score").alias("Average_Feedback_Score"),
                                max($"Feedback_Score").alias("Maximum_Feedback_Score"),
                                min($"Feedback_Score").alias("Minimum_Feedback_Score"))
    df2.show()

  spark.stop()
  }
}