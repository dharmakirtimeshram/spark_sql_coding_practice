import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q10_customer_feedback_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q10").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Feedback_Id","Customer_Name","Feedback_Score","Feedback_Date")

    val customer_feedback = Seq((1,"Alice",9,"2026-03-01"),(2,"Bob",8,"2026-03-05"),(3,"Charlie",6,"2026-03-10"),
                                 (4,"Dana",7,"2026-03-15"),(5,"Edward",10,"2026-03-20"),(6,"Fiona",6,"2026-03-25"))
    val df = spark.createDataFrame(customer_feedback).toDF(schema:_*)
    df.show()

    /* Create a new column feedback_type based on feedback_score:
             'Highly Satisfied' if feedback_score >= 9
             'Satisfied' if 7 >= feedback_score < 9
             'Neutral' if 5 >= feedback_score < 7
            'Dissatisfied' if feedback_score < 5
    ======================================================*/
    val feedback_type = df.withColumn("Feedback_Type",
                            when($"Feedback_Score" >= 9,"Highly Satisfied")
                              .when($"Feedback_Score" >= 7 && $"Feedback_Score" < 9,"Satisfied")
                              .when($"Feedback_Score" >= 5 && $"Feedback_Score" < 7,"Neutral")
                              .when($"Feedback_Score" < 5,"Dissatisfied"))
    feedback_type.show()

/* Filter records where feedback_date is in 'March 2026'.
============================================================== */
    val df1 = df.filter($"Feedback_Date".between("2026-03-01","2026-03-31"))
    df1.show()

/* Calculate the total (count), average (avg), maximum (max), and minimum (min) feedback_score for each feedback_type.
======================================================================================================================*/
    val df2 = feedback_type.groupBy("Feedback_Type")
                             .agg(count($"Feedback_Score").alias("Number of feedbacks"),
                               avg($"Feedback_Score").alias("Average_Feedback_Score"),
                               max($"Feedback_Score").alias("Maximum_Feedback_Score"),
                               min($"Feedback_Score").alias("Minimum_Feedback_Score"))
    df2.show()

  spark.stop()
  }
}