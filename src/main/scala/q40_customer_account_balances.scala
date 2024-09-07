import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q40_customer_account_balances {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q40").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Account_Id","Balance_Amount","Balance_Date")

    val account_balances = Seq((1,25000,"2028-06-01"),(2,15000,"2028-06-05"),(3,5000,"2028-06-10"),
                               (4,12000,"2028-06-15"),(5,30000,"2028-06-20"),(6,8000,"2028-06-25"))

    val df = spark.createDataFrame(account_balances).toDF(schema:_*)
    df.show()

    /* Create a new column balance_status based on balance_amount:
          o 'High Balance' if balance_amount > 20000
          o 'Medium Balance' if 10000 >= balance_amount <= 20000
          o 'Low Balance' if balance_amount < 10000
      ==============================================================*/
    val df1 = df.withColumn("Balance_Status",
                  when($"Balance_Amount" > 20000,"High Balance")
                  .when($"Balance_Amount" >= 10000 && $"Balance_Amount" <= 20000,"Medium Balance")
                  .when($"Balance_Amount" < 10000,"Low Balance"))
    df1.show()

/* Filter records where balance_date is in 'June 2028'.
============================================================= */

    val df2 = df.filter($"Balance_Date".between("2028-06-01","2028-06-30"))
    df2.show()


/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) balance_amount for each balance_status.
====================================================================================================================*/

    val df3 = df1.groupBy("Balance_Status")
                   .agg(sum($"Balance_Amount").alias("Total_Balance_Amount"),
                     avg($"Balance_Amount").alias("Average_Balance_Amount"),
                     max($"Balance_Amount").alias("Maximum_Balance_Amount"),
                     min($"Balance_Amount").alias("Minimum_Balance_Amount"))
    df3.show()

  spark.stop()
  }
}