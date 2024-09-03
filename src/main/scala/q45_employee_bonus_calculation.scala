import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q45_employee_bonus_calculation {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb45").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Employee_Id","Bonus_Amount","Bonus_Date")

    val manufacturing_cost = Seq((1,6000,"2025-01-01"),(2,2500,"2025-01-31"),(3,1500,"2025-01-10"),
                                 (4,3500,"2025-01-15"),(5,7000,"2025-01-20"),(6,1000,"2025-01-25"))

       val df = spark.createDataFrame(manufacturing_cost).toDF(schema:_*)
        df.show()

       /*  #Create a new column bonus_level based on bonus_amount:
              'High Bonus' if bonus_amount > 5000
             'Medium Bonus' if 2000 >= bonus_amount <= 5000
             'Low Bonus' if bonus_amount < 2000
    ===============================================*/
    val bonus_level = df.withColumn("Bonus Level",
                        when($"Bonus_Amount" > 5000,"High Bonus")
                          .when($"Bonus_Amount" >= 2000 && $"Bonus_Amount" <= 5000,"Medium Bonus")
                          .when($"Bonus_Amount" < 2000,"Low Bonus"))
    bonus_level.show()

/* #Filter records where bonus_date is in 'January 2025'.
========================================================*/
    val df1 = df.filter($"Bonus_Date".between("2025-01-01","2025-01-31"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) bonus_amount for each bonus_level.
   ==============================================================================================================*/
    val df2 = bonus_level.groupBy("Bonus Level")
                         .agg(sum($"Bonus_Amount").alias("Total Bonus Amount"),
                           avg($"Bonus_Amount").alias("Average Bonus Amount"),
                           max($"Bonus_Amount").alias("Maximum Bonus Amount"),
                           min($"Bonus_Amount").alias("Minimum Bonus Amount"))
      df2.show()

   spark.stop()
  }
}
