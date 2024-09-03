import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q30_product_return_analysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb30").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val product_returns = List((1,"Widget Pro", 6000,"2024-05-01"),(2,"Gadget Pro",3000,"2024-05-05"),
                             (3,"Widget Max",1500,"2024-05-10"),(4,"Gadget Max",2500,"2024-05-15"),
                             (5,"Widget Pro",2500,"2024-05-20"),(6,"Gadget Max",1000,"2024-05-25"))

    val schema = List("Return_Id","Product_Name","Return_Amount","Return_Date")

    val df = spark.createDataFrame(product_returns).toDF(schema:_*)
      df.show()

    /* #Create a new column return_status based on return_amount:
              'High' if return_amount > 5000
              'Medium' if 2000 >= return_amount <= 5000
              'Low' if return_amount < 2000
  ==================================================== */
    val return_status =df.withColumn("Return_Status",
                         when($"Return_Amount" > 5000,"High")
                           .when($"Return_Amount" > 5000 && $"Return_Amount","High")
                           .when($"Return_Amount" > 5000,"High"))
    return_status.show()

 /* #Filter returns where product_name ends with 'Pro'.
==================================================== */
    val df_filt = df.filter($"Product_Name".endsWith("Pro"))
    df_filt.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) return_amount for each return_status.
==================================================================================================================*/

    val df2 =return_status.groupBy("Return_Status")
                           .agg(sum($"Sales_Amount").alias("Total_Sales_Amount"),
                             avg($"Sales_Amount").alias("Average_Sales_Amount"),
                             max($"Sales_Amount").alias("Maximum_Sales_Amount"),
                             min($"Sales_Amount").alias("Minimum_Sales_Amount"))
    df2.show()

    spark.stop()
  }
}
