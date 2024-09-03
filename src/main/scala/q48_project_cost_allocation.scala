import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q48_project_cost_allocation {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb48").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Project_Id","Cost_Amount","Allocation_Date")

    val project_cost = Seq((1,35000,"2025-04-01"),(2,20000,"2025-04-05"),(3,12000,"2025-04-10"),
                           (4,25000,"2025-04-10"),(5,25000,"2025-04-15"),(6,14000,"2025-04-25"))
     val df = spark.createDataFrame(project_cost).toDF(schema:_*)
    df.show()

    /*  #Create a new column cost_category based on cost_amount:
   'High' if cost_amount > 30000
   'Medium' if 15000 >= cost_amount <= 30000
  'Low' if cost_amount < 15000
  ======================================================= */
    val cost_category = df.withColumn("Cost_Category",
                              when($"Cost_Amount" > 30000,"High")
                                .when($"Cost_Amount" >= 15000 && $"Cost_Amount" <= 30000,"Media")
                                .when($"Cost_Amount" < 15000,"Low"))
    cost_category.show()

/* #Filter records where allocation_date is in 'April 2025'.
=============================================================  */
    val df1 = df.filter($"Allocation_Date".between("2025-04-01","2025-04-30"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) cost_amount for each cost_category.
=================================================================================================================*/

    val df2 = cost_category.groupBy("Cost_Category")
                            .agg(sum($"Cost_Amount").alias("Total Cost Amount"),
                              avg($"Cost_Amount").alias("Average Cost Amount"),
                              max($"Cost_Amount").alias("Maximum Cost Amount"),
                              min($"Cost_Amount").alias("Minimum Cost Amount"))
          df2.show()
spark.stop()
  }
}
