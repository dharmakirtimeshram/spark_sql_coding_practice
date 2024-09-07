import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q30_corporate_event_attendance {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q30").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Event_Id","Employee_Id","Attendance_Status","Event_Date")

    val corporate_events = Seq((1,501,"Present","2027-08-01"),(2,502,"Excused","2027-08-05"),
                               (3,503,"Absent","2027-08-10"),(4,504,"Present","2027-08-15"),
                               (5,505,"Excused","2027-08-20"),(6,506,"Absent","2027-08-25"))
    val df = spark.createDataFrame(corporate_events).toDF(schema:_*)
    df.show()

    /*  Create a new column status_summary based on attendance_status:
           o 'Attended' if attendance_status is 'Present'
           o 'Excused' if attendance_status is 'Excused'
           o 'Absent' if attendance_status is 'Absent'
    =============================================================== */
    val df1 = df.withColumn("Status_Summary",
                            when($"Attendance_Status" === "Present", "Attended")
                              .when($"Attendance_Status" === "Excused" ,"Excused")
                              .when($"Attendance_Status" === "Absent","Absent"))
    df1.show()

/*  Filter records where event_date is in 'August 2027'.
=========================================================  */
    val df2 = df.filter($"Event_Date".between("2027-08-01","2027-08-31"))
    df2.show()

/* Calculate the total count of each status_summary.
=======================================================*/

    val df3 = df1.groupBy("Status_Summary").count()
    df3.show()

spark.stop()
  }
}