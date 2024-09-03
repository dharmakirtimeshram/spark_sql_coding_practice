import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q33_customer_purchase_history {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb33").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Purchase_Id","Customer_Id","Purchase_Amount","Purchase_Date")
    val customer_purchase = Seq((1,1,2500,"2024-08-01"),(2,2,1500,"2024-08-05"),(3,3,800,"2024-08-10"),
                             (4,4,3000,"2024-08-15"),(5,5,1200,"2024-08-20"),(6,6,5000,"2024-08-25"))

    val df = spark.createDataFrame(customer_purchase).toDF(schema:_*)
    df.show()

    /* # Create a new column purchase_level based on purchase_amount:
   'High' if purchase_amount > 2000
   'Medium' if 1000 >= purchase_amount <= 2000
  'Low' if purchase_amount < 1000
  ============================================================== */
    val purchase_level = df.withColumn("Purchase_level",
                           when($"Purchase_Amount" > 2000,"High")
                             .when($"Purchase_Amount" >= 2000 && $"Purchase_Amount" <= 2000,"Medium")
                             .when($"Purchase_Amount" < 1000,"Low"))
    purchase_level.show()

/* #Filter purchases where purchase_date is in 'August 2024'.
==============================================================  */
    val df1 = df.filter(col("Purchase_Date").between("2024-08-01","2024-08-31"))

    df1.show()

/* # Calculate the total (sum), average (avg), maximum (max), and minimum (min) purchase_amount for each purchase_level.
=====================================================================================================================*/
    val df2 = purchase_level.groupBy("Purchase_Level")
                              .agg(sum($"Purchase_Amount").alias("Total Purchase Amount"),
                                avg($"Purchase_Amount").alias("Average Purchase Amount"),
                                max($"Purchase_Amount").alias("Maximum Purchase Amount"),
                                min($"Purchase_Amount").alias("Minimum Purchase Amount"))
    df2.show()

  spark.stop()
  }
}
