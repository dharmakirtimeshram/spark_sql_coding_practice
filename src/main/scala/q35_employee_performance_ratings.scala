import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q35_employee_performance_ratings {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb35").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Employee_Id","Rating_Score","Rating_Date")

    val performance_rating = Seq((1,95,"2024-10-01"),(2,85,"2024-10-05"),(3,65,"2024-10-15"),
                                 (4,75,"2024-10-20"),(5,90,"2024-10-20"),(6,80,"2024-10-25"))
      val data = spark.sparkContext.parallelize(performance_rating)
     val df = spark.createDataFrame(data).toDF(schema:_*)

       df.show()

    /* #Create a new column rating_category based on rating_score:
          'Excellent' if rating_score >= 90
          'Good' if 70 >= rating_score < 90
          'Needs Improvement' if rating_score < 70
   ===================================================================== */

    val rating_category = df.withColumn("Rating_Category",
                             when($"Rating_Score" >= 90,"Excellent")
                               .when($"Rating_Score" >= 70 && $"Rating_Score" < 90,"Good")
                               .when($"Rating_Score" < 70,"Needs Improvement"))
    rating_category.show()

/* # Filter ratings where rating_date is in 'October 2024'.
=========================================================== */
    val df1 = df.filter($"Rating_Date".between("2024-10-01","2024-10-31"))
    df1.show()

/* #Calculate the total (count) of each rating_category.
======================================================================*/
    val df2 =rating_category.groupBy("Rating_Category").agg(count($"Rating_Category").alias("Total"))

           df2.show()

    spark.stop()
  }
}
