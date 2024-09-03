import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q23_emp_training_Records {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb23").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val training_records = List((1,1,50,"Tech"),(2,2,25,"Tech"),(3,3,15,"Management"),(4,4,35,"Tech"),
                                 (5,5,45,"Tech"),(6,6,30,"HR"))
    val col2 = Seq("Record_Id", "Employee-id " , "Training_Hours" , "Training_Type")
    val data = spark.sparkContext.parallelize(training_records)
    val df = data.toDF(col2:_*)
     df.show()

    /* # Create a new column training_status based on training_hours:
   'Extensive' if training_hours > 40
   'Moderate' if 20 >= training_hours <= 40
   'Minimal' if training_hours < 20
   ================================================== */
    val training_status = df.withColumn("Training_Status",
                    when(($"Training_Hours") > 40, "Extensive")
                       .when($"Training_Hours" >= 20 && $"Training_Hours" <= 40, "Moderate")
                        .when($"Training_Hours" < 20, "Minimal"))
    training_status.show()

/* #Filter records where training_type starts with 'Tech'.
============================================================ */

    val filt_df = df.filter($"Training_Type".startsWith("Tech"))
      filt_df.show()
/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) training_hours for each training_status.
=================================================================================================================== */

    val calc_df = training_status.groupBy("Training_Status")
                                 .agg(sum($"Training_Hours").alias("Total Working Hours"),
                                   avg($"Training_Hours").alias("Average Working Hours"),
                                   max($"Training_Hours").alias("Maximum Working Hours"),
                                   min($"Training_Hours").alias("Minium Working Hours"))
      calc_df.show()
   spark.stop()
  }

}
