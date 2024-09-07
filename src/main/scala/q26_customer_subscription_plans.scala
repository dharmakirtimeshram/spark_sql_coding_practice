import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q26_customer_subscription_plans {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q26").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Subscription_Id","Customer_Id","Subscription_Plan","Subscription_Amount")

    val customer_subscription = Seq((1,301,"Gold Standard",8000),(2,302,"Gold Premium",6000),(3,303,"Silver Basic",4500),
                               (4,304,"Gold Premium",10000),(5,305,"Gold Premium",15000),(6,306,"Bronze Basic",2000))

    val df = spark.createDataFrame(customer_subscription).toDF(schema:_*)
     df.show()


    /* Create a new column subscription_type based on subscription_amount:
             o 'Premium' if subscription_amount > 10000
             o 'Standard' if 5000 >= subscription_amount <= 10000
             o 'Basic' if subscription_amount < 5000
  ===================================================================================*/
    val df1 = df.withColumn("Subscription_Level",
                   when($"Subscription_Amount" > 10000,"Premium")
                     .when($"Subscription_Amount" >= 5000 && $"Subscription_Amount" <= 10000,"Standard")
                     .when($"Subscription_Amount" < 5000,"Basic"))
    df1.show()

/* Filter records where subscription_plan starts with 'Gold'.
======================================================================== */
    val df2 = df.filter($"Subscription_Plan".startsWith("Gold"))
    df2.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) subscription_amount for each subscription_type.
=============================================================================================================================*/
    val df3 = df1.groupBy("Subscription_Level")
                   .agg(sum($"Subscription_Amount").alias("Total_Amount"),
                     avg($"Subscription_Amount").alias("Average_Amount"),
                     max($"Subscription_Amount").alias("Maximum_Amount"),
                     min($"Subscription_Amount").alias("Minimum_Amount"))
    df3.show()

  spark.stop()
  }
}