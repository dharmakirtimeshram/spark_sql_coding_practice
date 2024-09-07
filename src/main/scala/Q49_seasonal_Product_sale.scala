import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q49_seasonal_Product_sale {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q49").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Product_Id","Sales_Amount","Season","Sales_Date")

    val seasonal_sale = Seq((1,80000,"Winter 2029","2029-12-01"),(2,40000,"Winter 2029","2029-12-05"),
                            (3,20000,"Winter 2029","2029-12-10"),(4,10000,"Winter 2029","2029-12-15"),
                            (5,50000,"Winter 2029","2029-12-20"),(6,12000,"Winter 2029","2029-12-25"))

    val df = spark.createDataFrame(seasonal_sale).toDF(schema:_*)
    df.show()

    /* Create a new column sales_performance based on sales_amount:
           o 'Excellent' if sales_amount > 70000
           o 'Good' if 30000 >= sales_amount <= 70000
           o 'Average' if 15000 >= sales_amount < 30000
           o 'Poor' if sales_amount < 15000
     ========================================================== */
    val df1 = df.withColumn("Sales_Performance",
                  when($"Sales_Amount" > 70000,"Excellent")
                  .when($"Sales_Amount" >= 30000 && $"Sales_Amount" <= 70000,"Good")
                  .when($"Sales_Amount" >= 15000  && $"Sales_Amount" < 30000,"Average")
                  .when($"Sales_Amount" < 15000,"Poor"))
    df1.show()

/* Filter records where season is 'Winter 2029'.
==================================================== */

    val df2 = df.filter($"Season".contains("Winter 2029"))
    df2.show()

/* For each sales_performance, calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount.
======================================================================================================================*/

    val df3 = df1.groupBy("Sales_Performance")
                   .agg(
                     sum($"Sales_Amount").alias("Total_Sales_Amount"),
                     avg($"Sales_Amount").alias("Average_Sales_Amount"),
                     max($"Sales_Amount").alias("Maximum_Sales_Amount"),
                     min($"Sales_Amount").alias("Minimum_Sales_Amount")
                   )
    df3.show()

spark.stop()
  }
}
