import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q47_customer_purchase_history {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q47").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Customer_Id","Purchase_Amount","Purchase_Date")

    val  customer_purchases = Seq((1,12000,"2029-01-01"),(2,6000,"2029-01-05"),(3,3000,"2029-01-10"),
                                  (4,9000,"2029-01-15"),(5,15000,"2029-01-20"),(6,2000,"2029-01-25"))

    val df = spark.createDataFrame(customer_purchases).toDF(schema:_*)
    df.show()

   /* Create a new column purchase_category based on purchase_amount:
            o 'High' if purchase_amount > 10000
            o 'Medium' if 5000 >= purchase_amount <= 10000
            o 'Low' if purchase_amount < 5000
        ====================================================== */
    val df1 = df.withColumn("Purchase_Category",
                   when($"Purchase_Amount" > 10000, "High")
                   when($"Purchase_Amount" >= 5000 && $"Purchase_Amount" <= 10000, "Medium")
                   when($"Purchase_Amount" < 5000, "Low"))
    df1.show()

   /*  Create a new column month extracted from purchase_date.
      ================================================================ */

    val df2 = df1.withColumn("Month",month($"Purchase_Date"))
    df2.show()

   /*   Filter records where purchase_date is in 'January 2029'.
   ================================================================== */

    val df3 = df.filter($"Purchase_Date".between("2029-01-01","2029-01-31"))
    df3.show()


    /*  For each month, calculate the total (sum), average (avg), maximum (max), and minimum (min)
             purchase_amount for each purchase_category.
    ====================================================================================================*/

    val df4 = df2.groupBy("Month","Purchase_Category")
                   .agg(
                     sum($"Purchase_Amount").alias("Total_Purchase_Amount"),
                     avg($"Purchase_Amount").alias("Average_Purchase_Amount"),
                     max($"Purchase_Amount").alias("Maximum_Purchase_Amount"),
                     min($"Purchase_Amount").alias("Minimum_Purchase_Amount")
                   )
     df4.show()

    spark.stop()
  }
}
