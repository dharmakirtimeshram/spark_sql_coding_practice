import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q4_vendor_payments_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q4").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Payment_Id","Vendor_Name","Payment_Amount","Payment_Date")

    val vendor_payments = Seq((1,"TechLLC",18000,"2025-09-01"),(2,"FinLLC",7000,"2025-09-05"),
                             (3,"RetailLLC",4000,"2025-09-10"),(4,"TechLLC",12000,"2025-09-15"),
                             (5,"TechLLC",20000,"2025-09-20"),(6,"FoodLLC",2500,"2025-09-25"))
    val df = spark.createDataFrame(vendor_payments).toDF(schema:_*)
    df.show()

    /* #Create a new column payment_type based on payment_amount:
          'High' if payment_amount > 15000
          'Medium' if 5000 <= payment_amount <= 15000
          'Low' if payment_amount < 5000
          =========================================  */
    val payment_type = df.withColumn("Payment_Type",
                         when($"Payment_Amount" > 15000, "High")
                           .when($"Payment_Amount" >= 5000 && $"Payment_Amount" <= 15000, "Medium")
                           .when($"Payment_Amount" < 5000, "Low"))
     payment_type.show()

/* # Filter records where vendor_name ends with 'LLC'.
   ===================================================== */
    val df1 =df.filter($"Vendor_Name".endsWith("LLC"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) payment_amount for each payment_type.
=================================================================================================================*/

    val df2 = payment_type.groupBy("Payment_Type")
                             .agg(sum($"Payment_Amount").alias("Total_Payment_Amount"),
                               avg($"Payment_Amount").alias("Average_Payment_Amount"),
                               max($"Payment_Amount").alias("Maximum_Payment_Amount"),
                               min($"Payment_Amount").alias("Minimum_Payment_Amount"))
    df2.show()
spark.stop()
  }
}
