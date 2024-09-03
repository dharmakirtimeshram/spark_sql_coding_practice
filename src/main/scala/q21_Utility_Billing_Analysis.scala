import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q21_Utility_Billing_Analysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb21").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val utility_bills = List((1,1,250,"2024-02-05"),(2,2,80,"2024-02-10"),(3,3,150,"2024-02-15"),
                             (4,4,220,"2024-02-20"),(5,5,90,"2024-02-25"),(6,6,300,"2024-02-28"))
                            .toDF("Bii_Id","Customer_Id","Bill_Amount","Billing_Date")
       utility_bills.show()

    /* #Create a new column bill_status based on bill_amount:
  'High' if bill_amount > 200
  'Medium' if 100 >= bill_amount <= 200
 'Low' if bill_amount < 100
 ==============================================*/
    val bill_status = utility_bills.withColumn("Bill_Status",
                                   when($"Bill_Amount" > 200, "High")
                                   .when($"Bill_Amount" >= 100 && $"Bill_Amount" <= 200, "Medium")
                                     .when($"Bill_Amount" < 100, "Low"))
    bill_status.show()

 /* #Filter bills where billing_date is in 'February 2024'.
========================================================= */
    val filt = utility_bills.filter($"Billing_Date".between("2024-02-01","2024-02-29"))
    filt.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) bill_amount for each bill_status.
===============================================================================================================*/
    val calc = bill_status.groupBy("Bill_Status")
                                         .agg(sum($"Bill_Amount").alias("Total Bill Amount"),
                                           avg($"Bill_Amount").alias("Average Bill Amount"),
                                           max($"Bill_Amount").alias("Maximum Bill Amount"),
                                           min($"Bill_Amount").alias("Minimum Bill Amount"))
     calc.show()

    spark.stop()
  }
}

