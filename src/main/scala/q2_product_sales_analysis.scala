import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q2_product_sales_analysis {
  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder().appName("qb2").master("local[4]").getOrCreate()
    import spark.implicits._

    val data = List((1,"Widget",700,"2024-01-15"),(2,"Gadget",150,"2024-01-20"),(3,"Widget",350,"2024-02-15"),
                    (4,"Device",600,"2024-02-20"),(5,"Widget",100,"2024-03-05"),(6,"Gadget",500,"2024-03-12"),
                    (7,"Device",300,"2024-04-12"),(8,"Gadget",500,"2024-04-28"),(9,"Widget",300,"2024-05-09"),
                    (10,"Gadget",250,"2024-05-25"),(11,"Device",600,"2024-06-10"),(12,"Gadget",400,"2024-06-20"))
                   .toDF("Sale_id","Product_name","Sale_amount","sale_date")
     //data.show()

    /*# Create a new column sale_category based on sale_amount:
   'High' if sale_amount > 500
   'Medium' if 200 >= sale_amount <= 500
   'Low' if sale_amount < 200
   ============================================================== */
    val sale_category = data.withColumn("Sale Category",when(($"Sale_amount")>500,"High")
                                    .when(($"Sale_amount") >= 200 && ($"Sale_amount") <=500,"Medium")
                                    .when(($"Sale_amount") < 200,"Low"))
         sale_category.show()

    /* #Filter sales where product_name ends with 't'.
       =============================================================== */
      val filt = data.filter(($"Product_name").endsWith("t"))
       filt.show()

    /* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) sale_amount for each month.
    ========================================================================================================== */

     val sale_month = data.withColumn("sale_month", month($"sale_date"))
    val calc =sale_month.groupBy("sale_month").agg(sum($"Sale_amount").alias("Total amount"),
                                             avg($"Sale_amount").alias("Average amount"),
                                             max($"Sale_amount").alias("Maximum amount"),
                                             min($"Sale_amount").alias("Minimum amount"))
       calc.show()

    spark.stop()
  }

}
