import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q27_employee_leave_records {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb27").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val leave_records = List((1,"Sick",12,"2024-01-10"),(2,"Sick",7,"2024-01-15"),(3,"Sick",3,"2024-02-20"),
                            (4,"Sick",6,"2024-02-25"),(5,"Sick",2,"2024-03-05"),(6,"Casual",5,"2024-03-12"))
                      .toDF("Employee_Id","Leave_Type","Leave_Duration_Days","Leave_Date")
       leave_records.show()

    /* #Create a new column leave_category based on leave_duration_days:
     'Extended' if leave_duration_days > 10
     'Short' if 5 <= leave_duration_days <= 10
     'Casual' if leave_duration_days < 5
     ================================================= */
    val leave_category = leave_records.withColumn("Leave_Category",
                                       when($"Leave_Duration_days" > 10,"Extended")
                                       .when($"Leave_Duration_days" >=  5 && $"Leave_Duration_days" <= 10 ,"Short")
                                        .when($"Leave_Duration_days" < 5 ,"Casual"))
    leave_category.show()

/* #Filter records where leave_type starts with 'Sick'.
========================================================= */
    val filt_df = leave_records.filter($"Leave_Type".contains("Sick"))
      filt_df.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) leave_duration_days for each leave_category.
==========================================================================================================================*/

    val calc_df = leave_category.groupBy("Leave_Category")
                                     .agg(sum($"Leave_Duration_days").alias("Total Leave Duration Days"),
                                       avg($"Leave_Duration_days").alias("Average Leave Duration Days"),
                                       max($"Leave_Duration_days").alias("Maximum Leave Duration Days"),
                                       min($"Leave_Duration_days").alias("Minimum Leave Duration Days"))
    calc_df.show()
    spark.stop()
  }
}
