import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q10_emp_attendance_tracking {
  def main(args:Array[String]):Unit={
    val sparkconf = new SparkConf().setAppName("qb10").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._
    val employee = List((1,"2024-01-10",9,"Sick"),(2,"2024-01-11",7,"Scheduled"),(3,"2024-01-12",8,"Sick"),
                   (4,"2024-01-13",8,"Scheduled"),(5,"2024-01-14",6,"Sick"),(6,"2024-01-15",8,"Scheduled"))
                 .toDF("Emp_Id","Attendance_Date","Hours_Worked","Attendance_Type")
      // employee.show()

    /*#Create a new column attendance_status based on hours_worked:
  'Full Day' if hours_worked >= 8
  'Half Day' if hours_worked < 8
  ==========================================*/
    val attendance_status = employee.withColumn("Attendance_Status",when(($"Hours_Worked")>=8,"Full Day")
                                                                   .when(($"Hours_Worked")<8,"Half Day"))
    attendance_status.show()

    /*#Filter attendance records where attendance_type starts with 'S'.
    =================================================================*/
    val filt = employee.filter(($"Attendance_Type").startsWith("S"))
    filt.show()

    /*#Calculate the total (sum), average (avg), maximum (max), and minimum (min) hours_worked for each attendance_status.
    =======================================================================================================================*/

    val calc =attendance_status.groupBy("Attendance_Status")
                                         .agg(sum($"Hours_Worked").alias("Total Hours Worked"),
                                           avg($"Hours_Worked").alias("Average Worked Hours"),
                                           max($"Hours_Worked").alias("Maximum Hours Worked"),
                                           min($"Hours_Worked").alias("Minimum Hours Worked"))
      calc.show()

      spark.stop()
  }

}
