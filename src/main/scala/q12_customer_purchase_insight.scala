import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q12_customer_purchase_insight {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q12").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Customer_Id","Purchase_Amount","Purchase_Date")

    val customer_purchase = Seq((1,12000,"2026-04-01"),(2,7000,"2026-04-05"),(3,3000,"2026-04-10"),
                               (4,15000,"2026-04-15"),(5,8000,"2026-04-20"),(6,2000,"2026-04-25"))
    val df = spark.createDataFrame(customer_purchase).toDF(schema:_*)

    df.show()

   /* Create a new column purchase_category based on purchase_amount:
    'High Value' if purchase_amount > 10000
   'Medium Value' if 5000 >= purchase_amount <= 10000
   'Low Value' if purchase_amount < 5000
  ================================================================*/
    val purchase_category = df.withColumn("Purchase_Category",
                               when($"Purchase_Amount" > 10000,"High Value")
                                 .when($"Purchase_Amount" >= 5000 && $"Purchase_Amount" <= 10000,"Medium Value")
                                 .when($"Purchase_Amount" < 5000,"Low Value"))
    purchase_category.show()

/* Filter records where purchase_date is in 'April 2026'.
================================================================ */
    val df1 = df.filter($"Purchase_Date".between("2026-04-01","2026-04-30"))
    df1.show()
/*  Calculate the total (sum), average (avg), maximum (max), and minimum (min) purchase_amount for each purchase_category.
============================================================================================================================*/

    val df2 = purchase_category.groupBy("Purchase_Category")
                                .agg(sum($"Purchase_Amount").alias("Total_Purchase_Amount"),
                                  avg($"Purchase_Amount").alias("Average_Purchase_Amount"),
                                  max($"Purchase_Amount").alias("Maximum_Purchase_Amount"),
                                  min($"Purchase_Amount").alias("Minimum_Purchase_Amount"))
    df2.show()

    spark.stop()
  }
}
