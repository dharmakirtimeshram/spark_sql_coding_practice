import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q34_client_feedback_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q34").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Feedback_Id","Client_Id","Feedback_Score","Feedback_Date")

    val client_feedback = Seq((1,601,9,"2027-12-01"),(2,602,8,"2027-12-05"),(3,603,6,"2027-12-10"),
                              (4,604,4,"2027-12-15"),(5,605,10,"2027-12-20"),(6,606,7,"2027-12-25"))

    val df = spark.createDataFrame(client_feedback).toDF(schema:_*)
    df.show()

    /* Create a new column feedback_type based on feedback_score:
         o 'Highly Positive' if feedback_score >= 9
         o 'Positive' if 7 >= feedback_score < 9
         o 'Neutral' if 5 >= feedback_score < 7
         o 'Negative' if feedback_score < 5
      ===================================================== */
    val df1 = df.withColumn("Feedback_Type",
                  when($"feedback_Score" >= 9, "Highly Positive")
                 .when($"feedback_Score" >= 7 && $"feedback_Score" < 9, "Positive")
                  .when($"feedback_Score" >= 5 && $"feedback_Score" < 7, "Neutral")
                  .when($"feedback_Score" < 5, "Negative"))
    df1.show()

/* Filter records where feedback_date is in 'December 2027'.
=============================================================== */

    val df2 = df.filter($"Feedback_Date".between("2027-12-01","2027-12-31"))
    df2.show()

/* Calculate the total count of each feedback_type.
============================================================*/

    val df3 = df1.groupBy("Feedback_Type").count().alias("Number_of_Feedback")
    df3.show()

spark.stop()
  }
}
