import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q29_monthly_sales_analysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb29").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val monthly_sales = List((1,"Widget",12000,"2024-04-01"),(2,"Gadget",6000,"2024-04-05"),
                             (3,"Widget Pro",3000,"2024-04-10"),(4,"Gadget Pro",8000,"2024-04-15"),
                             (5,"Widget Max",15000,"2024-04-20"),(6,"Gadget Max",4000,"2024-04-25"))

    val schema = List("Sales_Id","Product_Name","Sales_Amount","Sales_Date")
    val df = monthly_sales.toDF(schema:_*)
       df.show()

    /*  #Create a new column sales_performance based on sales_amount:
  'Excellent' if sales_amount > 10000
  'Good' if 5000 >= sales_amount <= 10000
  'Poor' if sales_amount < 5000
  ============================================= */
    val df_col = df.withColumn("Sales Performance",
                                when($"Sales_Amount" > 10000,"Excellent")
                                  .when($"Sales_Amount" <= 10000 && $"Sales_Amount" >= 5000,"Good")
                                  .when($"Sales_Amount" < 5000,"Poor"))
    df_col.show()

/* #Filter records where sales_date is in 'April 2024'.
==================================================== */
    val df_filt = df.filter($"Sales_Date".between("2024-04-01","2024-04-30"))
    df_filt.show()

/* # Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount for each sales_performance.
======================================================================================================================== */

    val df_calc = df_col.groupBy("Sales Performance")
                          .agg(sum($"Sales_Amount").alias("Total Sales Amount"),
                            avg($"Sales_Amount").alias("Average Sales Amount"),
                            max($"Sales_Amount").alias("Maximum Sales Amount"),
                           min($"Sales_Amount").alias("minimum Sales Amount"))

       df_calc.show()

    spark.stop()
  }
}
