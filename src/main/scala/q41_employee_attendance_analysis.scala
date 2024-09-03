import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q41_employee_attendance_analysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb41").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val employee_attendance = List((1,"2024-09-01",9),(2,"2024-09-02",5),(3,"2024-09-03",3),
                                    (4,"2024-09-04",8),(5,"2024-09-05",2),(6,"2024-09-6",7))

    val schema = List("employee_Id","Attendance_Date","Hours_Worked")
    val df = spark.createDataFrame(employee_attendance).toDF(schema:_*)
    df.show()

    /*  #Create a new column attendance_status based on hours_worked:
             'Full Day' if hours_worked >= 8
             'Half Day' if 4 <= hours_worked < 8
              'Absent' if hours_worked < 4
     =========================================================*/
    val attendance_status =df.withColumn("Attendance_Status",
                             when($"Hours_Worked" >= 8," Full Day")
                               .when($"Hours_Worked" >= 4 && $"Hours_Worked" < 8," Half Day")
                               .when($"Hours_Worked" < 4," Absent"))

    attendance_status.show()

/* #Filter records where attendance_date is in 'September 2024'.
=============================================================== */
    val df1 = df.filter($"Attendance_Date".between("2024-09-01","2024-09-30"))
    df1.show()
/* #Calculate the total (count), average (avg), maximum (max), and minimum (min) hours_worked for each attendance_status.
=======================================================================================================================*/
    val df2 = attendance_status.groupBy("Attendance_Status")
                                .agg(count($"Hours_Worked").alias("Number of Worked Hours"),
                                  avg($"Hours_Worked").alias("Average Worked Hours"),
                                  max($"Hours_Worked").alias("Maximum Worked Hours"),
                                  min($"Hours_Worked").alias("Minimum Worked Hours"))
    df2.show()

spark.stop()
  }
}