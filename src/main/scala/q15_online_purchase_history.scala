import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q15_online_purchase_history{
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb15").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._
     val purchase_data = List((1,1,700,"2024-02-05"),(2,2,150,"2024-02-10"),(3,3,150,"2024-02-10"),(4,4,400,"2024-02-20"),
                      (5,5,250,"2024-02-25"),(6,6,1000,"2024-02-28"))
                      .toDF("Purchase_Id","Customer_Id","Purchase_Amount","Purchase_Date")

    /*# Create a new column purchase_status based on purchase_amount:
  'Large' if purchase_amount > 500
 'Medium' if 200 >= purchase_amount <= 500
 'Small' if purchase_amount < 200
 ===========================================================================*/

    val purchase_status = purchase_data.
                                          withColumn("Purchase_Status",
                                            when(($"Purchase_amount")>500,"Large")
                                          .when(($"Purchase_amount")>=200 && ($"Purchase_amount")<=500,"Medium")
                                          .when(($"Purchase_amount")<200,"Small"))
    purchase_status.show()

/*#Filter purchases that occurred in the 'February 2024'.*/
     val filt = purchase_data.filter(($"Purchase_Date").between("2024-02-01","2024-02-29"))

    filt.show()

/*#Calculate the total (sum), average (avg), maximum (max), and minimum (min) purchase_amount for each purchase_status.
*/

    val calc = purchase_status.groupBy("Purchase_Status")
                                          .agg(sum($"Purchase_Amount").alias("Total Purchase Amount"),
                                            avg($"Purchase_Amount").alias("Average Purchase Amount"),
                                            max($"Purchase_Amount").alias("Maximum Purchase Amount"),
                                            min($"Purchase_Amount").alias("Minimum Purchase Amount"))

    calc.show()

   spark.stop()
  }
}