import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object q46_product_manufacturing_costs {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb46").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Product_Id","Manufacturing_Cost","Cost_Date")
    val manufacturing_cost =Seq((1,22000,"2025-02-05"),(2,15000,"2025-02-05"),(3,8000,"2025-02-10"),
                                (4,12000,"2025-02-15"),(5,18000,"2025-02-20"),(6,9000,"2025-02-25"))
    val df = spark.createDataFrame(manufacturing_cost).toDF(schema:_*)
    df.show()

    /* #Create a new column cost_category based on manufacturing_cost:
     'Expensive' if manufacturing_cost > 20000
     'Moderate' if 10000 >= manufacturing_cost <= 20000
     'Cheap' if manufacturing_cost < 10000
     =================================================  */
    val cost_category = df.withColumn("Cost_Category",
                          when($"Manufacturing_Cost" > 20000,"Expensive")
                            .when($"Manufacturing_Cost" >= 10000 && $"Manufacturing_Cost" <= 20000,"Moderate")
                            .when($"Manufacturing_Cost" < 10000,"Cheap"))
    cost_category.show()

/* #Filter records where cost_date is in 'February 2025'.
===================================================================== */
    val df1 = df.filter($"Cost_Date".between("2025-02-01","2025-02-29"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) manufacturing_cost for each cost_category.
======================================================================================================================*/

    val df2 = cost_category.groupBy("Cost_Category")
                            .agg(sum($"Manufacturing_Cost").alias("Total Manufacturing Cost"),
                              avg($"Manufacturing_Cost").alias("Average Manufacturing Cost"),
                              max($"Manufacturing_Cost").alias("Maximum Manufacturing Cost"),
                              min($"Manufacturing_Cost").alias("Minimum Manufacturing Cost"))
    df2.show()

spark.stop()
  }
}
