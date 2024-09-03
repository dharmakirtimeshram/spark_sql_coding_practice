import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q37_staff_level_Expenses {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb37").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Expense_Id","staff_Id","Travel_Amount","Travel_Date")

    val travel_expense = Seq((1,1,5500,"2024-12-01"),(2,2,2500,"2024-12-05"),(3,3,1200,"2024-12-10"),
                            (4,4,3000,"2024-12-15"),(5,5,4000,"2024-12-20"),(6,6,6000,"2024-12-25"))

     val df = travel_expense.toDF(schema:_*)
      df.show()

    /* #Create a new column travel_category based on travel_amount:
     'High' if travel_amount > 5000
     'Medium' if 2000 >= travel_amount <= 5000
      'Low' if travel_amount < 2000
      ======================================   */
    val travel_category = df.withColumn("Travel_Category",
                                when($"Travel_Amount" > 5000,"High")
                                  .when($"Travel_Amount" >= 2000 && $"Travel_Amount" <= 5000,"Medium")
                                  .when($"Travel_Amount" < 2000,"Low"))
          travel_category.show()

/* #Filter expenses where travel_date is in 'December 2024'.
============================================================== */
    val df1 = df.filter($"Travel_date".between("2024-12-01","2024-12-31"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) travel_amount for each travel_category.
=====================================================================================================================*/
    val df2 = travel_category.groupBy("Travel_Category")
                             .agg(sum($"Travel_Amount").alias("Total_Travel_Amount"),
                              avg ($"Travel_Amount").alias("Average_Travel_Amount"),
                               max($"Travel_Amount").alias("Maximum_Travel_Amount"),
                               min($"Travel_Amount").alias("Minimum_Travel_Amount"))

    df2.show()

   spark.stop()
  }
}
