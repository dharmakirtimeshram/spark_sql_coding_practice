import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q36_employee_bonus_allocation {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q36").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Employee_Id","Bonus_Amount","Bonus_Date")

    val employee_bonus = Seq((1,2500,"2028-02-01"),(2,1500,"2028-02-05"),(3,500,"2028-02-10"),
                              (4,1200,"2028-02-15"),(5,3000,"2028-02-20"),(6,800,"2028-02-25"))

    val df = spark.createDataFrame(employee_bonus).toDF(schema:_*)
    df.show()

 /* Create a new column bonus_category based on bonus_amount:
          o 'High' if bonus_amount > 2000
          o 'Medium' if 1000 >= bonus_amount <= 2000
          o 'Low' if bonus_amount < 1000
     ======================================================== */

    val df1 = df.withColumn("Bonus_Category",
                  when($"Bonus_Amount" > 2000,"High")
                  .when($"Bonus_Amount" >= 1000 && $"Bonus_Amount" <= 2000,"Medium")
                  .when($"Bonus_Amount" < 1000,"Low"))

    df1.show()

/* Filter records where bonus_date is in 'February 2028'.
================================================================ */
    val df2 = df.filter($"Bonus_Date".between("2028-02-01","28-02-29"))
    df2.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) bonus_amount for each bonus_category.
====================================================================================================================*/

    val df3 = df1.groupBy("Bonus_Category")
                   .agg(sum($"Bonus_Amount").alias("Total_Bonus_Amount"),
                     avg($"Bonus_Amount").alias("Average_Bonus_Amount"),
                     max($"Bonus_Amount").alias("Maximum_Bonus_Amount"),
                     min($"Bonus_Amount").alias("Minimum_Bonus_Amount"))
    df3.show()

    spark.stop()
  }
}
