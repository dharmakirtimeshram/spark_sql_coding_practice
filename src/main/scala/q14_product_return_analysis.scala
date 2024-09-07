import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q14_product_return_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q14").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Return_Id","Product_Category","Return_Amount","Return_Date")

    val product_returns = Seq((1,"Electronics",6000,"2026-06-01"),(2,"Apparel",3000,"2026-06-05"),
                              (3,"Furniture",1500,"2026-06-10"),(4,"Electronics",4000,"2026-06-15"),
                              (5,"Furniture",7000,"2026-06-20"),(6,"Apparel",1800,"26-06-25"))
    val df = spark.createDataFrame(product_returns).toDF(schema:_*)
    df.show()

    /*  Create a new column return_type based on return_amount:
                 o 'High' if return_amount > 5000
                 o 'Medium' if 2000 >= return_amount <= 5000
                 o 'Low' if return_amount < 2000
    =============================================================== */
    val return_type = df.withColumn("Return_Type",
                           when($"Return_Amount" > 5000,"High")
                             .when($"Return_Amount" >= 2000 && $"Return_Amount" <=5000, "Medium")
                             .when($"Return_Amount" < 2000,"Low"))
    return_type.show()

 /*  Filter records where return_date is in 'June 2026'.
 =========================================================*/
    val df1 = df.filter($"Return_Date".between("2026-06-01","2026-06-30"))
    df1.show()

/*  Calculate the total (sum), average (avg), maximum (max), and minimum (min) return_amount for each return_type.
=====================================================================================================================*/

    val df2 = return_type.groupBy($"Return_Type")
                          .agg( sum($"Return_Amount").alias("Total_Return_Amount"),
                            avg($"Return_Amount").alias("Average_Return_Amount"),
                            max($"Return_Amount").alias("Max_Return_Amount"),
                            min($"Return_Amount").alias("Min_Return_Amount"))
    df2.show()

spark.stop()
  }
}
