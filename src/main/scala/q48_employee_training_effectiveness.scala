import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q48_employee_training_effectiveness {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q48").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Employee_Id","Training_Hours","Pre_Score","Past_Score","Training_Date")

    val employee_training = Seq((1,15,70,95,"2029-02-01"),(2,10,65,80,"2029-02-05"),(3,12,55,70,"2029-02-10"),
                               (4,20,80,100,"2029-02-15"),(5,8,60,75,"2029-02-20"),(6,18,90,95,"2029-02-25"))

    val df = spark.createDataFrame(employee_training).toDF(schema:_*)
    df.show()

  /* Create a new column score_increase calculated as:
         o post_score - pre_score
    ==============================================*/

    val df1 = df.withColumn("Score_Increase", $"Past_Score" - $"Pre_Score")
    df1.show()

/* Create a new column training_effectiveness based on score_increase:
         o 'High' if score_increase >= 20
         o 'Medium' if 10 >= score_increase < 20
         o 'Low' if score_increase < 10
     ====================================================== */
    val df2 = df1.withColumn("Training_Effectiveness",
                  when($"Score_Increase" >= 20, "High")
                  when($"Score_Increase" >= 10 && $"Score_Increase" < 20, "Medium")
                  when($"Score_Increase" < 10, "Low"))
    df2.show()

/* Filter records where training_date is in 'February 2029'
===================================================================== */

    val df3 = df.filter($"Training_Date".between("2029-02-01","2029-02-28"))
    df3.show()

/* For each training_effectiveness, calculate the total count of employee_id and the average training_hours.
============================================================================================================*/

    val df4 = df2.groupBy("Training_Effectiveness")
                   .agg(
                     count($"Employee_Id").alias("Number_Of_employees"),
                     avg($"Training_Hours").alias("Average_Training_Average")
                   )
    df4.show()

    spark.stop()
  }
}