import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q25_training_attendance_record {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q25").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Training_Id","Participant_Id","Hours_Attended","Attendance_Date")

    val data = Seq((1,201,45,"2027-05-01"),(2,202,30,"2027-05-05"),(3,203,15,"2027-05-10"),
                   (4,204,35,"2027-05-15"),(5,205,50,"2027-05-20"),(6,206,10,"2027-05-25"))

    val training_attendance  = spark.createDataFrame(data).toDF(schema:_*)
    training_attendance.show()

    /* Create a new column attendance_level based on hours_attended:
            o 'Full' if hours_attended >= 40
            o 'Partial' if 20 >= hours_attended < 40
            o 'Minimal' if hours_attended < 20
    ======================================================================== */
    val attendance_level = training_attendance.withColumn("Attendance_Level",
                                               when($"Hours_Attended" >= 40,"Full")
                                                 .when($"Hours_Attended" >= 20 && $"Hours_Attended" < 40,"Partial")
                                                 .when($"Hours_Attended" < 20,"Minimal"))

    attendance_level.show()

/* Filter records where attendance_date is in 'May 2027'.
===================================================================*/
    val may_attendance = training_attendance.filter($"Attendance_Date".between("2027-05-01","2027-05-31"))
    may_attendance.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) hours_attended for each attendance_level.
======================================================================================================================*/
    val attendance_calc = attendance_level.groupBy("Attendance_Level")
                                           .agg(sum($"Hours_Attended").alias("Total_Hours_Attended"),
                                             avg($"Hours_Attended").alias("Average_Hours_Attended"),
                                             max($"Hours_Attended").alias("Maximum_Hours_Attended"),
                                             min($"Hours_Attended").alias("minimum_Hours_Attended"))
    attendance_calc.show()

spark.stop()
  }
}
