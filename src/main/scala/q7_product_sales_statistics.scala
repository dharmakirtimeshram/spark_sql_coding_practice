import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q7_product_sales_statistics {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q7").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Product_Id","Sales_Amount","Sales_Date")

    val product_sales = Seq((1,28000,"2025-12-01"),(2,12000,"2025-12-05"),(3,8000,"2025-12-10"),
                           (4,15000,"2025-12-15"),(5,30000,"2025-12-20"),(6,6000,"2025-12-25"))
    val df =spark.createDataFrame(product_sales).toDF(schema:_*)
     df.show()

    /* #Create a new column sales_category based on sales_amount:
   'Top Seller' if sales_amount > 25000
    'Average Seller' if 10000 >= sales_amount <= 25000
    'Low Seller' if sales_amount < 10000
    ==============================================================*/
    val sales_category = df.withColumn("Sales_Category",
                             when($"Sales_Amount" > 25000,"Top Seller")
                               .when($"Sales_Amount" >= 10000 && $"Sales_Amount" <= 25000,"Average Seller")
                               .when($"Sales_Amount" < 10000,"Low Seller"))
    sales_category.show()

/* #Filter records where sales_date is in 'December 2025'.
===============================================================  */
    val df1 = df.filter($"Sales_Date".between("2025-12-01","2025-12-31"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount for each sales_category.
=================================================================================================================*/

    val df2 = sales_category.groupBy("Sales_Category")
                              .agg(sum($"Sales_Amount").alias("Total_Sales_Amount"),
                                avg($"Sales_Amount").alias("Average_Sales_Amount"),
                                max($"Sales_Amount").alias("Maximum_Sales_Amount"),
                                min($"Sales_Amount").alias("Minimum_Sales_Amount"))
    df2.show()

spark.stop()
  }
}