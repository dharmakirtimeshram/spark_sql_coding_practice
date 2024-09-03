import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q50_client_transactions {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb50").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Transaction_Id","Client_Name","Transaction_Amount","Transaction_Date")

    val client_transaction = Seq((1,"TechCorp",22000,"2025-06-01"),(2,"FinCorp",12000,"2025-06-05"),
                                  (3,"RetailCorp",8000,"2025-06-10"),(4,"TechCorp",15000,"2025-06-15"),
                                  (5,"TechCorp",25000,"2025-06-20"),(6,"FoodCorp",6000,"2025-06-25"))
    val df = spark.createDataFrame(client_transaction).toDF(schema:_*)
    df.show()

    /*  #Create a new column transaction_level based on transaction_amount:
           'High' if transaction_amount > 20000
           'Medium' if 10000 >= transaction_amount <= 20000
           'Low' if transaction_amount < 10000
           ==============================================  */
    val transaction_level = df.withColumn("Transaction_Level",
                               when($"Transaction_Amount" > 20000,"High")
                                 .when($"Transaction_Amount" >= 10000 && $"Transaction_Amount" <= 20000,"Medium")
                                 .when($"Transaction_Amount" < 10000,"Low"))
    transaction_level.show()

/* #Filter records where client_name ends with 'Corp'.
================================================================== */
    val df1 = df.filter($"Client_Name".endsWith("Corp"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) transaction_amount for each transaction_level.
=============================================================================================================================*/

    val df2 = transaction_level.groupBy("Transaction_Level")
                                 .agg(sum($"Transaction_Amount").alias("Total Transaction Amount"),
                                   avg($"Transaction_Amount").alias("Average Transaction Amount"),
                                   max($"Transaction_Amount").alias("Maximum Transaction Amount"),
                                   min($"Transaction_Amount").alias("Minimum Transaction Amount"))
    df2.show()

  spark.stop()
  }
}
