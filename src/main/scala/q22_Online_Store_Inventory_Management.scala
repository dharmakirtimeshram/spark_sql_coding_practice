import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q22_Online_Store_Inventory_Management {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb22").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val store_inventory =List((1,"Widget Lite",15,"2024-01-10"),(2,"Gadget",60,"2024-01-15"),(3,"Light Lite",25,"2024-01-25"),
                        (4,"Appliance",5,"2024-02-05"),(5,"Widget Pro",70,"2024-03-05"),(6,"Light Pro",45,"2024-03-12"))
                      .toDF("Product_Id","Product_Name","Quantity_in_Stock","Last_Restocked")
      store_inventory.show()

    /* #Create a new column stock_status based on quantity_in_stock:
          'Critical' if quantity_in_stock < 20
          'Low' if 20 >= quantity_in_stock < 50
          'Sufficient' if quantity_in_stock >= 50
   ======================================================*/
    val stock_status = store_inventory.withColumn("Stock_Status",
                                        when($"Quantity_in_Stock" < 20, "Critical")
                                          .when($"Quantity_in_Stock" >= 20 && $"Quantity_in_Stock" < 50, "Low")
                                          .when($"Quantity_in_Stock" >= 50, "Sufficient"))
    stock_status.show()

/* #Filter products where product_name ends with 'Lite'.
  ==============================================================   */
    val filt = store_inventory.filter($"Product_Name".endsWith("Lite"))
    filt.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) quantity_in_stock for each stock_status.
===================================================================================================================*/
    val calc = stock_status.groupBy("Stock_Status")
                                        .agg(sum($"Quantity_in_Stock").alias("Total Quantity in stock"),
                                          avg($"Quantity_in_Stock").alias("Average Quantity in stock"),
                                          max($"Quantity_in_Stock").alias("Maximum Quantity in stock"),
                                          min($"Quantity_in_Stock").alias("minimum Quantity in stock"))
        calc.show()


    spark.stop()
  }
}
