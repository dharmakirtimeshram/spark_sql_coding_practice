import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object qA2_product_analysis {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf()
      .set("spark.app.name","analysis")
      .set("spark.master","local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val data = List((1,"SmartPhone",700,"Electronics"),(2,"TV",1200,"Electronics"),
                  (3,"Shoes",150,"Apparel"),(4,"Socks",25,"Apparel"),(5,"Laptop",800,"Electronics"),
                  (6,"Jacket",200,"Apparel"),(7,"Shirt",100,"Apparel"),(8,"Speaker",150,"Electronics"),
                  (9,"Smartwatch",200,"Electronics"),(10,"Jeans",200,"Apparel"))
                  .toDF("Product_Id","Product_Name","Price","Category")

    //data.show()

    /* Create a new column price_category based on price:
           1)'Expensive' if price > 500
           2) 'Moderate' if 200 <= price <= 500
           3)'Cheap' if price < 200
           ============================================*/

    val category = data.select(col("Product_Id"),col("Product_Name"),col("Price"),col("Category"),
                       when(col("Price")>500,"Expensive")
                       .when(col("Price")>=200 && col("Price") <= 500," Moderate")
                       .when(col("Price")<200,"Cheap").alias("Price_category"))
            category.show()

    /* Filter products whose product_name starts with 'S'.
    ======================================================*/

    val srtwith = data.filter(col("Product_Name").startsWith("S"))
              srtwith.show()

    /*Filter products whose product_name ends with 's'.
    ==================================================== */

    val endwith = data.filter(col("Product_Name").endsWith("s"))
         endwith.show()

    /* Calculate the total price (sum), average price, maximum price, and minimum price for each category.
   ======================================================================================================  */
    val calc = data.groupBy("Category").agg(sum(col("Price")).alias(" Total Price"),
                                       avg(col("Price")).alias("Average Price"),
                                       max(col("Price")).alias(" Maximum Price"),
                                       min(col("Price")).alias("Minimum Price"))
                calc.show()




    spark.stop()
  }

}
