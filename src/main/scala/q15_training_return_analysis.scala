import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q15_training_return_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q15").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Program_Id","Training_Hours","Program_Date")

    val training_programs = Seq((1,45,"2026-07-01"),(2,25,"2026-07-05"),(3,15,"2026-07-10"),
                                 (4,35,"2026-07-15"),(5,50,"2026-07-20"),(6,10,"2026-07-25"))

    val df = spark.createDataFrame(training_programs).toDF(schema:_*)
    df.show()

/* Create a new column training_level based on training_hours:
   'Advanced' if training_hours > 40
   'Intermediate' if 20 >= training_hours <= 40
   'Basic' if training_hours < 20
 ======================================================= */
    val training_level = df.withColumn("Training_Level",
                             when($"Training_Hours" > 40 ,"Advanced")
                               .when($"Training_Hours" >= 20 && $"Training_Hours" <= 40 ,"Intermediate")
                               .when($"Training_Hours" < 20 ,"Basic"))
    training_level.show()

/* Filter records where program_date is in 'July 2026'.
=============================================================== */
    val df1 = df.filter($"Program_Date".between("2026-07-01","2026-07-31"))
    df1.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) training_hours for each training_level.
=====================================================================================================================*/
    val df2 = training_level.groupBy("Training_Level")
                              .agg(sum($"Training_Hours").alias("Total_Training_Hours"),
                                avg($"Training_Hours").alias("Average_Training_Hours"),
                                max($"Training_Hours").alias("Maximum_Training_Hours"),
                                min($"Training_Hours").alias("Minimum_Training_Hours"))
    df2.show()

  spark.stop()
  }
}