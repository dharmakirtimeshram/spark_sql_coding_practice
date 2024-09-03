import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q11_book_store_inventory {
  def main(args:Array[String]):Unit={
    val sparkconf = new SparkConf().setAppName("qb11").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val book_store = List((1," The Great gatsby",150,"2024-01-10"),(2,"The Catcher in the Rye",80,"2024-01-15"),
                           (3,"Moby Dick",200,"2024-01-20"),(4,"To Kill a Mockingbird",30,"2024-02-01"),
                           (5,"The Odyssey",60,"2024-02-10"),(6,"War and Peace",20,"2024-03-01"))
                          .toDF("Book_Id","Book_Title","Stock_Quantity","Last_Updated")
    //book_store.show()

/*#Create a new column stock_level based on stock_quantity:
 'High' if stock_quantity > 100
  'Medium' if 50 >= stock_quantity <= 100
 'Low' if stock_quantity < 50
 =========================================*/

    val stock_level =book_store.withColumn("Stock_Level",
                                            when(($"Stock_Quantity") >100,"High")
                                            .when(($"Stock_Quantity")>=50 && ($"Stock_Quantity")<=100,"Medium")
                                            .when(($"Stock_Quantity")<50,"Low"))

    stock_level.show()

    /*#Filter books where book_title starts with 'The'.
    ========================================================*/
    val filt = book_store.filter(($"Book_Title").startsWith("The"))
    filt.show()

/*#Calculate the total (sum), average (avg), maximum (max), and minimum (min) stock_quantity for each stock_level.
================================================================================================================*/
    val calc = stock_level.groupBy("Stock_Level").agg(sum($"Stock_Quantity").alias("Total Stock Quantity"),
                                               avg($"Stock_Quantity").alias("Average Stock Quantity"),
                                               max($"Stock_Quantity").alias("Maximum Stock Quantity"),
                                                min($"Stock_Quantity").alias("Minimum Stock Quantity"))
       calc.show()
    spark.stop()
  }

}
