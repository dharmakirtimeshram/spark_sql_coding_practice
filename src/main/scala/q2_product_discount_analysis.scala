import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q2_product_discount_analysis {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf().setAppName("q2").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = List("Discount_Id","Product_Name","Discount_Amount","Discount_Date")

    val product_discount = List((1,"Gadget A",3500,"2025-08-01"),(2,"Gadget B",2000,"2025-08-05"),
                                (3,"Widget A",1200,"2025-08-10"),(4,"Gadget C",2500,"2025-08-15"),
                                 (5,"Gadget D",4000,"2025-08-20"),(6,"Widget B",1000,"2025-08-25"))
    val df = spark.createDataFrame(product_discount).toDF(schema:_*)
    df.show()

    /* #Create a new column discount_level based on discount_amount:
    'High' if discount_amount > 3000
    'Medium' if 1500 >= discount_amount <= 3000
    'Low' if discount_amount < 1500
    =============================================  */
    val discount_level =df.withColumn("Discount_Level",
                            when($"Discount_Amount" > 3000,"High")
                              .when($"Discount_Amount" >= 1500 && $"Discount_Amount" <= 3000,"Medium")
                              .when($"Discount_Amount" < 1500,"Low"))
    discount_level.show()

/* #Filter records where product_name starts with 'Gadget'.
=============================================================  */
    val df1 = df.filter($"Product_Name".startsWith("Gadget"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) discount_amount for each discount_level.
=====================================================================================================================*/

    val df2 = discount_level.groupBy("Discount_Level")
                            .agg(sum($"Discount_Amount").alias("Total_Discount_Amount"),
                              avg($"Discount_Amount").alias("Average_Discount_Amount"),
                              max($"Discount_Amount").alias("Maximum_Discount_Amount"),
                              min($"Discount_Amount").alias("Minimum_Discount_Amount"))
    df2.show()

    spark.stop()
  }

}
