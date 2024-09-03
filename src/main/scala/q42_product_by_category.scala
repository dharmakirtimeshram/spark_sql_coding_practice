import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q42_product_by_category {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb42").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Return_Id","Product_Category","Return_Amount","Return_Date")

    val product_returns = Seq((1,"Electronics",12000,"2024-10-01"),(2,"Electronics",6000,"2024-10-05"),
                              (3,"Furniture",3000,"2024-10-10"),(4,"Electronics",7000,"2024-10-15"),
                               (5,"Electronics",15000,"2024-10-20"),(6,"Apparel",4000,"2024-10-25"))
    val df =spark.createDataFrame(product_returns).toDF(schema:_*)

    df.show()

    /*  #Create a new column return_level based on return_amount:
               'High' if return_amount > 10000
               'Medium' if 5000 >= return_amount <= 10000
               'Low' if return_amount < 5000
         ==============================================================*/
    val return_level = df.withColumn("Return_Level",
                          when($"Return_Amount" > 10000,"High")
                            .when($"Return_Amount" >= 5000 && $"Return_Amount"<= 10000,"Medium")
                            .when($"Return_Amount" < 5000,"Low"))
    return_level.show()

/* #Filter records where product_category ends with 'Electronics'.
=================================================================== */
    val df1 = df.filter($"Product_Category".endsWith("Electronics"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) return_amount for each return_level.
========================================================================================================================*/
    val df2 = return_level.groupBy("Return_Level")
                          .agg(sum($"Return_Amount").alias("Total Return Amount"),
                            avg($"Return_Amount").alias("Average Return Amount"),
                            max($"Return_Amount").alias("Maximum Return Amount"),
                            min($"Return_Amount").alias("Minimum Return Amount"))
    df2.show()

spark.stop()
  }
}
