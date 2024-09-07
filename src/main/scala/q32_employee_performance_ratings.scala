import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q32_employee_performance_ratings {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q32").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Employee_Id","Performance_Score","Review_Date")

    val emp_performance = Seq((1,95,"2027-10-01"),(2,80,"2027-10-05"),(3,65,"2027-10-10"),
                              (4,55,"2027-10-15"),(5,92,"2027-10-20"),(6,70,"2027-10-25"))

    val df = spark.createDataFrame(emp_performance).toDF(schema:_*)
    df.show()

    /*  Create a new column performance_category based on performance_score:
         o 'Exceptional' if performance_score >= 90
         o 'Good' if 75 >= performance_score < 90
         o 'Satisfactory' if 60 >= performance_score < 75
         o 'Needs Improvement' if performance_score < 60
     ============================================================ */
    val df1 = df.withColumn("Performance_Category",
                 when($"Performance_Score" >= 90,"Exceptional")
                   .when($"Performance_Score" >= 75 && $"Performance_Score" < 90,"Good")
                   .when($"Performance_Score" >= 60 && $"Performance_Score" < 75,"Satisfactory")
                   .when($"Performance_Score" < 60,"Needs Improvement"))
    df1.show()

/* Filter records where review_date is in 'October 2027'.
=============================================================== */

    val df2 = df.filter($"Review_Date".between("2027-10-01","2027-10-31"))
    df2.show()

/* Calculate the total count of each performance_category.
================================================================*/
    val df3 = df1.groupBy("Performance_Category").count()
    df3.show()

spark.stop()
  }
}