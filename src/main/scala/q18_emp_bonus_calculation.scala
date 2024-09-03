import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q18_emp_bonus_calculation {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb18").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val employee_bonus = List((1,"Sales Department",2500,"2024-01-10"),(2,"Marketing Department",1500,"2024-01-15"),
                    (3,"IT Department",800,"2024-01-20"),(4,"HR Department",1200,"2024-02-01"),
                    (5,"Sales Department",1800,"2024-02-10"),(6,"IT Department",950,"2024-03-01"))
    val  df = spark.createDataFrame(employee_bonus).toDF("Employee_id", "Department", "Bonus", "Bonus_Date")
         //df.show()

    /*#Create a new column bonus_category based on bonus:
  'High' if bonus > 2000
  'Medium' if 1000 >= bonus <= 2000
  'Low' if bonus < 1000
  ===================================================== */
    val bonus_category = df.withColumn("Bonus_Category",
                                               when($"Bonus" >2000,"High")
                                               .when($"Bonus">=1000 && $"Bonus"<= 2000,"Medium")
                                                 .when($"Bonus"<1000,"Low"))
    bonus_category.show()

/*# Filter bonuses where department ends with 'Department'.
======================================================================*/

    val filt_df = df.filter(($"Department").endsWith("Department"))
    filt_df.show()

/*#Calculate the total (sum), average (avg), maximum (max), and minimum (min) bonus for each bonus_category.
=============================================================================================================*/

    val calc_df = bonus_category.groupBy("Bonus")
                            .agg(sum($"Bonus").alias("Total Bonus"),
                              avg($"Bonus").alias("Average Bonus"),
                              max($"Bonus").alias("Maximum Bonus"),
                              min($"Bonus").alias("Minimum Bonus"))
    calc_df.show()

   spark.stop()
  }
}
