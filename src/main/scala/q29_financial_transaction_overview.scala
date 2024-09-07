import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q29_financial_transaction_overview {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q29").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Transaction_Id","Transaction_Amount","Transaction_type","Transaction_Date")

    val financial_transaction = Seq((1,15000,"Debit","2027-07-01"),(2,7000,"Credit","2027-07-05"),
                                    (3,3000,"Debit","2027-07-10"),(4,12000,"Credit","2027-07-15"),
                                    (5,18000,"Debit","2027-07-20"),(6,2500,"Credit","2027-07-25"))
    val df = spark.createDataFrame(financial_transaction).toDF(schema:_*)
    df.show()

    /* Create a new column amount_category based on transaction_amount:
         o 'High' if transaction_amount > 10000
         o 'Medium' if 5000 >= transaction_amount <= 10000
         o 'Low' if transaction_amount < 5000
     ============================================================= */
    val df1 = df.withColumn("Amount_Category",
                  when($"Transaction_Amount" > 10000,"High")
                    .when($"Transaction_Amount" >= 5000 && $"Transaction_Amount" <= 10000,"Medium")
                     .when($"Transaction_Amount" < 5000,"Low"))
    df1.show()

/* Filter records where transaction_date is in 'July 2027'.
=============================================================== */
    val df2 = df.filter($"Transaction_Date".between("2027-07-01","2027-07-31"))
    df2.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) transaction_amount for each amount_category.
===========================================================================================================================*/

    val df3 = df1.groupBy("Amount_Category")
                  .agg(sum($"Transaction_Amount").alias("Total Amount"),
                    avg($"Transaction_Amount").alias("Average Amount"),
                    max($"Transaction_Amount").alias("Maximum Amount"),
                    min($"Transaction_Amount").alias("Minimum Amount"))
    df3.show()

    spark.stop()
  }
}
