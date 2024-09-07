import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q11_employee_attendance_analysi {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q11").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Employee_Id","Department","Attendance_Days","Attendance_Month")

    val employee_attendance = Seq((1,"HR",25,"2026-03"),(2,"IT",18,"2026-03"),(3,"Sales",7,"2026-03"),
                                 (4,"IT",20,"2026-03"),(5,"HR",22,"2026-03"),(6,"Sales",12,"2026-03"))
    val df = spark.createDataFrame(employee_attendance).toDF(schema:_*)
    df.show()

    /* Create a new column attendance_category based on attendance_days:
       'Excellent' if attendance_days >= 22
       'Good' if 15 >= attendance_days < 22
       'Average' if 8 >= attendance_days < 15
       'Poor' if attendance_days < 8
    ====================================================== */
    val attendance_category = df.withColumn("Attendance_Category",
                                 when($"Attendance_Days" >= 22,"Excellent")
                                   .when($"Attendance_Days" >= 15 && $"Attendance_Days" < 22,"Good")
                                   .when($"Attendance_Days" >= 8 && $"Attendance_Days" < 15,"Average")
                                   .when($"Attendance_Days" <15, "Poor"))
    attendance_category.show()

/* Filter records where attendance_month is 'March 2026'.
================================================================= */
    val df1 = df.filter($"Attendance_Month".between("2026-03","2026-03"))
    df1.show()


/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) attendance_days for each attendance_category.
==========================================================================================================================*/
   val df2 = attendance_category.groupBy("Attendance_Category")
                                   .agg(sum($"Attendance_Days").alias("Total_Attendance_Days"),
                                     avg($"Attendance_Days").alias("Average_Attendance_Days"),
                                     max($"Attendance_Days").alias("Maximum_Attendance_Days"),
                                     min($"Attendance_Days").alias("Minimum_Attendance_Days"))

    df2.show()

  spark.stop()
  }
}
