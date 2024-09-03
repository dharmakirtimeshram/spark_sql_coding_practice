import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q20_event_Attendance_Tracking {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb20").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._
    val event_attendance = List((1,"Tech Conference",600,"2024-01-10"),(2,"Sports Event",250,"2024-01-15"),
                                (3,"Tech Expo",700,"2024-01-20"),(4,"Music Festival",150,"2024-02-01"),
                                 (5,"Tech Seminar",300,"2024-02-10"),(6,"Art Exhibition",400,"2024-03-01"))
                              .toDF("Event_Id","Event_Name","Attendance","Event_Date")
    event_attendance.show()

    /* #Create a new column attendance_status based on attendees:
  'Full' if attendees > 500
  'Moderate' if 200 >= attendees <= 500
  'Low' if attendees < 200
  ================================*/
    val attendance_status = event_attendance.withColumn("Attendance_Status",
                                            when($"Attendance" > 500,"Full")
                                              .when($"Attendance" >= 200 && $"Attendance" <= 500, " Moderate")
                                              .when($"Attendance" < 200,"Low"))
    attendance_status.show()

/*#Filter events where event_name starts with 'Tech'.
=====================================================*/
    val filt = event_attendance.filter(($"Event_Name").startsWith("Tech"))
        filt.show()

/*#Calculate the total (sum), average (avg), maximum (max), and minimum (min) attendees for each attendance_status.
================================================================================================================*/

    val calc = attendance_status.groupBy("Attendance")
                                       .agg(sum($"Attendance").alias("Total Attendance"),
                                         avg($"Attendance").alias("Average Attendance"),
                                         max($"Attendance").alias("Maximum Attendance"),
                                         min($"Attendance").alias("Minimum Attendance"))
    calc.show()

  spark.stop()
  }
}
