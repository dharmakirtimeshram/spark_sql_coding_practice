import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q18_course_enrollment_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q18").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Student_Id","Course_Name","Enrollment_Amount","Enrollment_Date")

    val course_enrollments = Seq((1,"Data Science",6000,"2026-10-01"),(2,"AI",3000,"2026-10-05"),
                               (3,"Web Dev",1500,"2026-10-10"),(4,"Data Science",4500,"2026-10-15"),
                               (5,"AI",7000,"2026-10-20"),(6,"Web Dev",1000,"2026-10-25"))
    val df = spark.createDataFrame(course_enrollments).toDF(schema:_*)
    df.show()

  /*  Create a new column enrollment_category based on enrollment_amount:
         o 'Premium' if enrollment_amount > 5000
         o 'Standard' if 2000 >= enrollment_amount <= 5000
         o 'Basic' if enrollment_amount < 2000
    ==========================================================*/

    val enrollment_category =df.withColumn("Enrollment_Category",
                                 when($"Enrollment_Amount" > 5000 ,"Premium")
                                   .when($"Enrollment_Amount" >= 2000 && $"Enrollment_Amount" <= 5000,"Standard")
                                   .when($"Enrollment_Amount" < 2000 ,"Basic"))
    enrollment_category.show()

  /*  Filter records where enrollment_date is in 'October 2026'.
    ============================================================*/

    val df1 = df.filter($"Enrollment_Date".between("2026-10-01","2026-10-31"))
    df1.show()

  /*  Calculate the total (sum), average (avg), maximum (max), and minimum (min) enrollment_amount for each enrollment_category.
    ==============================================================================================================================*/

    val df2 = enrollment_category.groupBy("Enrollment_Category")
                                  .agg(sum($"Enrollment_Amount").alias("Total_Enrollment_Amount"),
                                    avg($"Enrollment_Amount").alias("Average_Enrollment_Amount"),
                                    max($"Enrollment_Amount").alias("Maximum_Enrollment_Amount"),
                                    min($"Enrollment_Amount").alias("Minimum_Enrollment_Amount"))
  df2.show()

    spark.stop()
  }

}
