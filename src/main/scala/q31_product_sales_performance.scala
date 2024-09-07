import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q31_product_sales_performance {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q31").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Product_Id","Sales_Amount","Sales_Date","Sales_Channel")

    val product_sales = Seq((1,60000,"2027-09-01","Online"),(2,25000,"2027-09-05","Offline"),
                            (3,12000,"2027-09-10","Online"),(4,8000,"2027-09-15","Offline"),
                            (5,70000,"2027-09-20","Online"),(6,9000,"2027-09-25","Offline"))
    val df = spark.createDataFrame(product_sales).toDF(schema:_*)
    df.show()

    /* Create a new column sales_performance based on sales_amount:
        o 'Outstanding' if sales_amount > 50000
        o 'Good' if 20000 >= sales_amount <= 50000
        o 'Average' if 10000 >= sales_amount < 20000
        o 'Below Average' if sales_amount < 10000
    ======================================================= */
    val df1 =df.withColumn("Sales_Performance",
                 when($"Sales_Amount" > 50000,"Outstanding")
                   .when($"Sales_Amount" >= 20000 && $"Sales_Amount"  < 50000,"Good")
                   .when($"Sales_Amount" >= 10000 && $"Sales_Amount"  < 20000,"Average")
                   .when($"Sales_Amount" < 10000,"Below Average"))
    df1.show()

/*  Filter records where sales_date is in 'September 2027'.
=============================================================== */
    val df2 = df.filter($"Sales_Date".between("2027-009-01","2027-09-30"))
    df2.show()

/* For each sales_channel, calculate the total (sum), average (avg), maximum (max),
       and minimum (min) sales_amount for each sales_performance category.
====================================================================================*/

    val df3 = df1.groupBy("Sales_Performance")
                   .agg(sum($"Sales_Amount").alias("Total_sales_Amount"),
                     avg($"Sales_Amount").alias("Average_sales_Amount"),
                     max($"Sales_Amount").alias("Maximum_sales_Amount"),
                     min($"Sales_Amount").alias("Minimum_sales_Amount"))
    df3.show()

spark.stop()
  }
}