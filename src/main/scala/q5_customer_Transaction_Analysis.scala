import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q5_customer_Transaction_Analysis {
  def main(args:Array[String]):Unit={
    val sparkconf = new SparkConf().setAppName("qb5").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val data = List((1,1,1200,"2024-01-15"),(2,2,600,"2024-01-20"),(3,3,300,"2024-02-15"),(4,4,1500,"2024-02-05"),
                    (5,5,200,"2024-03-05"),(6,6,900,"2024-03-12"),(7,7,1200,"2024-04-12"),(8,8,700,"2024-04-28"),
                    (9,9,400,"2024-05-04"),(10,10,600,"2024-05-25"),(11,11,300,"2024-06-07"),(12,12,800,"2024-06-28"))
                    .toDF("Transaction_Id","Customer_Id","Transaction_Amount","Transaction_Date")
      //data.show()

    /* #Create a new column transaction_category based on transaction_amount:
  'High' if transaction_amount > 1000
  'Medium' if 500 >= transaction_amount <= 1000
  'Low' if transaction_amount < 500
  ==================================================================================*/

    val transaction_category = data.withColumn("Transaction_Category",when(($"Transaction_Amount") > 1000,"High")
                                   .when(($"Transaction_Amount") >=500 && ($"Transaction_Amount")<=1000,"Medium")
                                   .when(($"Transaction_Amount") <500,"Low"))
        transaction_category.show()

    /* #Filter transactions where transaction_date is in 2024.
    =========================================================== */

    val filt = data.filter(($"Transaction_Date").contains("2024"))

          filt.show()

    /* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) transaction_amount for each category.
    =================================================================================================================== */

    val calc = transaction_category.groupBy("Transaction_Category")
                                       .agg(sum($"Transaction_Amount").alias("Total Transaction Amount"),
                                         avg($"Transaction_Category").alias("Average Transaction Amount"),
                                         max($"Transaction_Amount").alias("Maximum Transaction Amount"),
                                         min($"Transaction_Amount").alias("Minimum Transaction Amount"))
               calc.show()


   spark.stop()
  }

}
