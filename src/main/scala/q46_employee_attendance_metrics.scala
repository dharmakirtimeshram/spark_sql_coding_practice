import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q46_employee_attendance_metrics {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q46").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Employee_Id","Days_Present","Days_Absent","Attendance_Month")

    val employee_attendance = Seq((1,22,2,"2028-12-01"),(2,18,7,"2028-12-05"),(3,15,10,"2028-12-15"),
                                  (4,25,1,"2028-12-20"),(5,20,5,"2028-12-20"),(6,12,12,"2028-12-25"))

    val df = spark.createDataFrame(employee_attendance).toDF(schema:_*)
    df.show()

    /*Create a new column total_days calculated as:
             o days_present + days_absent
         ==================================================*/
    val df1 = df.withColumn("Total_Days",$"Days_Present" + $"Days_Absent")
    df1.show()


  /* Create a new column attendance_rate calculated as:
            o days_present / total_days
    ====================================================*/

    val df2 = df1.withColumn("Attendance_Rate", $"Days_Present" / $"Total_Days")
    df2.show()

    /* Create a new column attendance_status based on attendance_rate:
            o 'Excellent' if attendance_rate >= 0.9
            o 'Good' if 0.75 >= attendance_rate < 0.9
            o 'Average' if 0.5 >= attendance_rate < 0.75
            o 'Poor' if attendance_rate < 0.5
     =================================================================== */

    val df3 = df2.withColumn("Attendance_Status",
                 when($"Attendance_Rate" >= 0.9, "Excellent")
                 when($"Attendance_Rate" >= 0.75 && $"Attendance_Rate" <0.9, "Good")
                 when($"Attendance_Rate" >= 0.5 && $"Attendance_Rate" < 0.75, "Average")
                 when($"Attendance_Rate" <  0.5, "Poor"))

    df3.show()


/* Filter records where attendance_month is in 'December 2028'.
===================================================================== */

    val df4 =df.filter($"Attendance_Month".between("2028-12-01","2028-12-31"))
    df4.show()

/* For each attendance_status, calculate the total count of employee_id.
================================================================================*/

    val df5 = df3.groupBy("Attendance_Status").count().alias("Number_of_Employees")
    df5.show()

spark.stop()
  }
}
