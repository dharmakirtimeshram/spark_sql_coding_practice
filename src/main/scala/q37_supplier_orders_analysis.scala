import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q37_supplier_orders_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q37").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Order_Id","Supplier_Id","Order_Amount","Order_Date")

    val  supplier_orders = Seq((1,701,60000,"2028-03-01"),(2,702,30000,"2028-03-05"),(3,703,15000,"2028-03-10"),
                               (4,704,40000,"2028-03-15"),(5,705,15000,"2028-03-20"),(6,706,12000,"2028-03-25"))

    val df = spark.createDataFrame(supplier_orders).toDF(schema:_*)
    df.show()

    /* Create a new column order_status based on order_amount:
         o 'High Value' if order_amount > 50000
         o 'Medium Value' if 20000 >= order_amount <= 50000
         o 'Low Value' if order_amount < 20000
============================================================= */

    val df1 = df.withColumn("Order_Status",
                 when($"Order_Amount" > 50000,"High Value")
                 when($"Order_Amount" >= 20000 && $"Order_Amount" <= 50000 ,"Medium Value")
                 when($"Order_Amount" < 20000,"Low Value"))

    df1.show()

/* Filter records where order_date is in 'March 2028'.
============================================================ */

    val df2 = df.filter($"Order_Date".between("2028-03-01","2028-03-31"))
    df2.show()

/* For each supplier_id, calculate the total (sum), average (avg), maximum (max),
             and minimum (min) order_amount for each order_status.
====================================================================================*/

    val df3 = df1.groupBy("Order_Status")
                   .agg(sum($"Order_Amount").alias("Total_Order_Amount"),
                     avg($"Order_Amount").alias("Average_Order_Amount"),
                     max($"Order_Amount").alias("Maximum_Order_Amount"),
                     min($"Order_Amount").alias("Minimum_Order_Amount"))

    df3.show()

  spark.stop()
  }
}
