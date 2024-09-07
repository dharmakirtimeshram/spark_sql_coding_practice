import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q16_employee_bonus_calculation {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q16").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Employee_Id","Bonus_Amount","Bonus_Date")

    val employee_bonuses = Seq((1,12000,"2026-08-05"),(2,6000,"2026-08-05"),(3,4000,"2026-08-10"),
                               (4,8000,"2026-08-15"),(5,15000,"2026-08-20"),(6,2500,"2026-08-25"))

    val df = spark.createDataFrame(employee_bonuses).toDF(schema:_*)
    df.show()

    /*Create a new column bonus_category based on bonus_amount:
              'High' if bonus_amount > 10000
              'Medium' if 5000 >= bonus_amount <= 10000
              'Low' if bonus_amount < 5000
          =========================================================== */
    val bonus_category = df.withColumn("Bonus_Category",
                            when($"Bonus_Amount" > 10000,"High")
                              .when($"Bonus_Amount" >= 5000 && $"Bonus_Amount" <= 10000,"Medium")
                              .when($"Bonus_Amount" < 5000,"Low"))
    bonus_category.show()

/*  Filter records where bonus_date is in 'August 2026'.
================================================================*/
    val df1 = df.filter($"Bonus_Date".between("2026-08-01","2026-08-31"))
    df1.show()

/*  Calculate the total (sum), average (avg), maximum (max), and minimum (min) bonus_amount for each bonus_category.
====================================================================================================================*/

    val df2 = bonus_category.groupBy("Bonus_Category")
                             .agg(sum($"Bonus_Amount").alias("Total_Bonus_Amount"),
                               avg($"Bonus_Amount").alias("Average_Bonus_Amount"),
                               max($"Bonus_Amount").alias("Maximum_Bonus_Amount"),
                               min($"Bonus_Amount").alias("Minimum_Bonus_Amount"))
    df2.show()

    spark.stop()
  }
}
