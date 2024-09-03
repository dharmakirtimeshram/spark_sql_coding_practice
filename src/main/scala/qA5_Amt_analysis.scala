import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object qA5_Amt_analysis {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf()
      .set("spark.app.name","q5")
      .set("spark.master","local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val data = List((1,"2023-01-15",1200,"Credit"),(2,"2023-11-15",600,"Debit"),(3,"2023-12-20",400,"Credit"),
                    (4,"2023-10-10",1500,"Debit"),(5,"2023-12-30",250,"Credit"),(6,"2023-09-25",700,"Debit"),
                    (7,"2023-09-13",800,"Credit"),(8,"2023-08-22",450,"Debit"),(9,"2023-08-26",1500,"Credit"),
                    (10,"2023-01-30",900,"Debit"),(11,"2023-05-28",1500,"Credit"),(12,"2023-08-16",500,"Debit"))
                         .toDF("Transaction_id","Transaction_date","Amount","Transaction_type")

    // data.show()

    /*#Create a new column amount_category based on amount:
   'High' if amount > 1000
   'Medium' if 500 >= amount <= 1000
   'Low' if amount < 500
   ====================================================================*/

    val amt_category = data.withColumn("Amount_category",when(col("Amount")> 1000,"High")
                                .when(col("Amount")>=500 && col("Amount")<=1000,"Medium")
                                .when(col("Amount")<500,"Low"))
    amt_category.show()

    /*#Create a new column transaction_month that extracts the month from transaction_date.
     =================================================================================*/

    val tran_month = data.withColumn("Transaction_month",month(col("Transaction_date")))
    tran_month.show()

    /*#Filter transactions that occurred in the month of 'December'.
    ================================================================= */

    val filt =tran_month.filter(col("Transaction_month") !== "12")

    filt.show()

    /*#Calculate the total (sum), average (avg), maximum (max), and minimum (min) amount for each transaction_type.
    ================================================================================================================     */

    val calc = data.groupBy("Transaction_type").agg(sum($"Amount").alias("Total Amount"),
                                               avg($"Amount").alias("Average Amount"),
                                                max($"Amount").alias("Maximum Amount"),
                                               min($"Amount").alias("Minimum Amount"))
    calc.show()

spark.stop()
  }

}
