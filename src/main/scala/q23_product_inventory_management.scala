import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q23_product_inventory_management {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q23").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Product_Id","Stock_Quantity","Restock_Date")

    val product_inventory = Seq((1,120,"2027-03-01"),(2,70,"2027-03-05"),(3,40,"2027-03-10"),
                                  (4,90,"2027-03-15"),(5,150,"2027-03-20"),(6,30,"2027-03-25"))

    val df = spark.createDataFrame(product_inventory).toDF(schema:_*)
    df.show()

    /*  Create a new column inventory_status based on stock_quantity:
                 o 'High' if stock_quantity > 100
                 o 'Medium' if 50 >= stock_quantity <= 100
                 o 'Low' if stock_quantity < 50
      ============================================================ */
     val inventory_status = df.withColumn("Inventory_Status",
                                when($"Stock_Quantity" > 100,"High")
                                  .when($"Stock_Quantity" >= 50 && $"Stock_Quantity" <= 100,"Medium")
                                  .when($"Stock_Quantity" < 50,"Low"))
    inventory_status.show()

/*  Filter records where restock_date is in 'March 2027'.
================================================================ */
    val df1 = df.filter($"Restock_Date".between("2027-03-01","2027-03-31"))
    df1.show()


/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) stock_quantity for each inventory_status.
======================================================================================================================*/

    val df2 = inventory_status.groupBy("Inventory_Status")
                                  .agg(sum($"Stock_Quantity").alias("Total_stock_Quantity"),
                                    avg($"Stock_Quantity").alias("Average_stock_Quantity"),
                                    max($"Stock_Quantity").alias("Maximum_stock_Quantity"),
                                    min($"Stock_Quantity").alias("Minimum_stock_Quantity"))
    df2.show()

  spark.stop()
  }
}

