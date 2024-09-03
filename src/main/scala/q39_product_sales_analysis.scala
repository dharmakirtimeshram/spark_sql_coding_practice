import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q39_product_sales_analysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb39").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Sale_Id","Product_Name","Sale_Amount","Sale_Date")

    val product_sales = Seq((1,"Tech Gadget",6000,"2025-02-01"),(2,"Tech Widget",3000,"2025-02-05"),
                            (3,"Home Gadget",1500,"2025-02-10"),(4,"Tech Tool",5000,"2025-02-15"),
                            (5,"Tech Device",7000,"2025-02-20"),(6,"Office Gadget",1800,"2025-02-25"))
    val df = product_sales.toDF(schema:_*)

    df.show()


    /* #Create a new column sales_category based on sale_amount:
     'High' if sale_amount > 5000
     'Medium' if 2000 >= sale_amount <= 5000
     'Low' if sale_amount < 2000
     ========================================== */
    val sales_category = df.withColumn("Sales_Category",
                           when($"Sale_Amount" > 5000, "High")
                             .when($"Sale_Amount" >= 2000 && $"Sale_Amount" <= 50000, "Medium")
                             .when($"Sale_Amount" < 2000, "Low"))
    sales_category.show()

/* #Filter sales where product_name starts with 'Tech'.
=======================================================*/
    val df1 = df.filter($"Product_Name".startsWith("Tech"))

    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) sale_amount for each sales_category.
==================================================================================================================*/

    val df2 = sales_category.groupBy("Sales_Category")
                               .agg(sum($"Sale_Amount").alias("Total Sale Amount"),
                                 avg($"Sale_Amount").alias("Average Sale Amount"),
                                 max ($"Sale_Amount").alias("Maximum Sale Amount"),
                                 min($"Sale_Amount").alias("Minimum Sale Amount"))
    df2.show()

   spark.stop()
  }
}
