import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q31_supplier_transactions {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb31").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema =List("Transaction_Id","Supplier_Name","Transaction_Amount","Transaction_Date")
    val supplier_transactions = List((1,"Alpha Ltd",16000,"2024-06-01"),(2,"Beta Inc",8000,"2024-06-10"),
                                      (3,"Gamma LLC",4000,"2024-06-10"),(4,"Delta Co",12000,"2024-06-15"),
                                       (5,"Epsilon Ltd",18000,"2024-06-20"),(6,"Zeta Corp",3000,"2024-06-25"))
    val data =spark.sparkContext.parallelize(supplier_transactions)
    val df = spark.createDataFrame(data).toDF(schema:_*)

    df.show()

    /* #Create a new column transaction_status based on transaction_amount:
     'High' if transaction_amount > 15000
     'Medium' if 5000 >= transaction_amount <= 15000
     'Low' if transaction_amount < 5000
     ======================================================= */
    val df2 = df.withColumn("Transaction_Status",
                        when($"Transaction_Amount" > 15000,"High")
                          .when($"Transaction_Amount" >= 5000 && $"Transaction_Amount" <= 15000,"Medium")
                          .when($"Transaction_Amount" < 5000,"Low"))
    df2.show()

/* #Filter transactions where transaction_date is in 'June 2024'.
================================================================= */
    val df3 = df.filter($"Transaction_date".between("2024-06-01","2024-06-30"))
    df3.show()
/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) transaction_amount for each transaction_status.
==============================================================================================================================*/

    val df4 = df2.groupBy("Transaction_Status")
                            .agg(sum($"Transaction_Amount").alias("Total Transaction Amount"),
                              avg($"Transaction_Amount").alias("Average Transaction Amount"),
                              max($"Transaction_Amount").alias("Maximum Transaction Amount"),
                              min($"Transaction_Amount").alias("Minimum Transaction Amount"))
    df4.show()
 spark.stop()
  }
}
