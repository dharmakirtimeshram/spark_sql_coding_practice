import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q9_customer_purchase_history {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf().setAppName("qb9").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val data = List((1,1,2500,"2024-01-15"),(2,2,1500,"2024-01-15"),(3,3,500,"2024-02-20"),
                    (4,4,2200,"2024-02-20"),(5,5,900,"2024-01-25"),(6,6,3000,"2024-03-12"))
                    .toDF("Purchase_Id","Customer_Id","Purchase_Amount","Purchase_Date")
       data.show()

    /*#Create a new column purchase_category based on purchase_amount:
  'Large' if purchase_amount > 2000
  'Medium' if 1000 >= purchase_amount <= 2000
  'Small' if purchase_amount < 1000
  ==================================================================*/

    val purchase_category = data.withColumn("Purchase_Category",
                                          when(($"Purchase_Amount")>2000,"Large")
                                          .when(($"Purchase_Amount") >=1000 && ($"Purchase_Amount")<=2000,"Medium")
                                          .when(($"Purchase_Amount")<1000,"Small"))
      purchase_category.show()

    /*# Filter purchases that occurred in 'January 2024'.
    ======================================================*/
    val mnth = data.withColumn("month",month($"Purchase_date"))
   val filt = mnth.filter(($"month").contains(1))
          filt.show()

    /*# Calculate the total (sum), average (avg), maximum (max), and minimum (min) purchase_amount for each purchase_category.
========================================================================================================================= */

    val calc = purchase_category.groupBy("Purchase_Category").agg(sum($"Purchase_Amount").alias("Total purchase Amount"),
                                                              avg($"Purchase_Amount").alias("Average Purchase Amount"),
                                                              max($"Purchase_Amount").alias("Maximum Purchase Amount"),
                                                              min($"Purchase_Amount").alias("Minimum Purchase Amount"))
     calc.show()

      spark.stop()
  }

}
