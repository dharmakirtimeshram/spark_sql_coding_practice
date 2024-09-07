import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q3_client_subscription_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q3").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Subscription_Id","Subscription_Amount","Subscription_Date")

    val client_subscriptions = Seq((1,6000,"2025-08-01"),(2,3000,"2025-08-05"),(3,1500,"2025-08-10"),
                                    (4,4000,"2025-08-10"),(5,5500,"2025-08-20"),(6,10000,"2025-08-25"))
    val df = spark.createDataFrame(client_subscriptions).toDF(schema:_*)

    df.show()

    /* #Create a new column subscription_level based on subscription_amount:
    'Premium' if subscription_amount > 5000
    'Standard' if 2000 >= subscription_amount <= 5000
    'Basic' if subscription_amount < 2000
   ======================================================== */
    val subscription_level = df.withColumn("Subscription_Level",
                               when($"Subscription_Amount" > 5000,"Premium")
                                 .when($"Subscription_Amount" >= 2000 && $"Subscription_Amount" <= 5000,"Standard")
                                 .when($"Subscription_Amount" < 2000,"Basic"))
    subscription_level.show()

/* #Filter records where subscription_date is in 'August 2025'.
========================================================================= */
    val df1 = df.filter($"Subscription_Date".between("2025-08-01","2025-08-31"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) subscription_amount for each subscription_level.
=============================================================================================================================*/
    val df2 = subscription_level.groupBy("Subscription_Level")
                                  .agg(sum($"Subscription_Amount").alias("Total_Subscription_Amount"),
                                    avg($"Subscription_Amount").alias("Average_Subscription_Amount"),
                                    max($"Subscription_Amount").alias("Maximum_Subscription_Amount"),
                                    min($"Subscription_Amount").alias("Minimum_Subscription_Amount"))
    df2.show()

  spark.stop()
  }
}
