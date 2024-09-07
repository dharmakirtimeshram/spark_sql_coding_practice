import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q42_product_inventory_turnover {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q42").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Product_Id","Initial_Stock","Sales","Returns","Inventory_Date")

    val product_inventory = Seq((1,6000,1000,500,"2028-08-01"),(2,7000,2000,600,"2028-08-05"),
                                (3,5000,1500,300,"2028-08-10"),(4,8000,3000,800,"2028-08-15"),
                                (5,4000,3000,800,"2028-08-20"),(6,9000,2500,700,"2028-08-25"))

    val df = spark.createDataFrame(product_inventory).toDF(schema:_*)
    df.show()

    /* Create a new column current_stock calculated as:
             o initial_stock - sales + returns
        =============================================== */

          val df1 = df.withColumn("Current_Stock", $"Initial_Stock" - $"Sales" + $"Returns")
          df1.show()


/* Create a new column stock_status based on current_stock:
         o 'Surplus' if current_stock > 5000
         o 'Stable' if 1000 >= current_stock <= 5000
         o 'Deficit' if current_stock < 1000
   ========================================================= */

    val df2 = df1.withColumn("Stock_Status",
                     when($"Current_Stock" > 5000, "Surplus")
                     when($"Current_Stock" >= 1000 && $"Current_Stock" <= 5000, "Stable")
                     when($"Current_Stock" < 1000, "Deficit"))

    df2.show()

/* Filter records where inventory_date is in 'August 2028'.
================================================================= */

    val df3 = df.filter($"Inventory_Date".between("2028-08-01","2028-08-31"))
    df3.show()

/* For each stock_status, calculate the total (sum), average (avg), maximum (max), and minimum (min) current_stock.
====================================================================================================================*/
    val df4 = df2.groupBy("Stock_Status")
                  .agg(sum($"Current_Stock").alias("Total_Current_Stock"),
                    avg($"Current_Stock").alias("Average_Current_Stock"),
                    max($"Current_Stock").alias("Maximum_Current_Stock"),
                    min($"Current_Stock").alias("Minimum_Current_Stock"))

    df4.show()

   spark.stop()
  }
}
