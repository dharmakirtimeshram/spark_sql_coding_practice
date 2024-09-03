import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q4_product_inventory_analysis {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf().setAppName("qb4").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val data = List((1,"Pro widget",30,"2024-01-10"),(2,"Pro Device",120,"2024-01-15"),(3,"Standard",200,"2024-01-20"),
                   (4,"Pro Gadget",40,"2024-02-20"),(5,"Standard",20,"2024-02-01"),(6,"Pro Device",90,"2024-03-01"),
                   (7,"Pro Gadget",150,"2024-03-06"),(8,"Pro Device",170,"2024-04-05"),(9,"Standard",50,"2024-04-16"),
                   (10,"Pro Widget",100,"2024-05-6"),(11,"Standard",60,"2024-05-15"),(12,"Pro Widget",250,"2024-05-28"))
                 .toDF("Product_Id","Product_Name","Stock","Last_Restocked")
    //data.show()

    /* #Create a new column stock_status based on stock:
 'Low' if stock < 50
  'Medium' if 50 >= stock <= 150
 'High' if stock > 150
======================================================     */

    val stock_status = data.withColumn("Stock_Status",when(($"Stock")<50,"Low")
                                        .when(($"Stock")>= 50 && ($"Stock") <= 150, "Medium")
                                         .when(($"Stock")>150,"High"))
              stock_status.show()

    /* #Filter products where product_name contains 'Pro'.
    =====================================================*/
    val filt = data.filter(($"Product_Name").contains("Pro"))

           filt.show()

    /* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) stock for each stock status.
    =========================================================================================================== */

    val calc = stock_status.groupBy("Stock_Status").agg(sum($"Stock").alias("Total Stock"),
                                                   avg($"Stock").alias("Average Stock"),
                                                   max($"Stock").alias("Maximum Stock"),
                                                  min($"Stock").alias("Minimum Stock"))
        calc.show()


    spark.stop()
  }

}
