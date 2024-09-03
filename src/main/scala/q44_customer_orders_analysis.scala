import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q44_customer_orders_analysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb44").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Order_Id","Order_Amount","Order_Date")

    val customer_orders = Seq((1,12000,"2024-11-01"),(2,7000,"2024-11-05"),(3,3000,"2024-11-10"),
                                (4,8000,"2024-11-15"),(5,15000,"2024-11-20"),(6,4000,"2024-11-25"))
       val df = spark.createDataFrame(customer_orders).toDF(schema:_*)
    df.show()

    /*  #Create a new column order_status based on order_amount:
             'High Value' if order_amount > 10000
              'Medium Value' if 5000 >= order_amount <= 10000
              'Low Value' if order_amount < 5000
   ======================================================= */
        val order_status = df.withColumn("Order_Status",
                              when($"Order_Amount" >= 10000,"High Value")
                                .when($"Order_Amount" >= 5000 && $"Order_Amount" <= 10000,"Medium Value")
                                .when($"Order_Amount" < 5000,"Low Value"))
            order_status.show()

/* #Filter records where order_date is in 'November 2024'.
===========================================================*/
    val df1 = df.filter($"Order_Date".between("2024-11-01","2024-11-31"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) order_amount for each order_status.
=====================================================================================================================*/
    val df2 = order_status.groupBy("Order_Status")
                 .agg(sum($"Order_Amount").alias("Total Order Amount"),
                   avg($"Order_Amount").alias("Average Order Amount"),
                   max($"Order_Amount").alias("Maximum Order Amount"),
                   min($"Order_Amount").alias("Minimum Order Amount"))
     df2.show()

   spark.stop()
  }
}
