import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q27_sales_target_achievement {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q27").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Salesperson_Id","Target_Amount","Achieved_Amount")

    val sales_target = Seq((1,60000,65000),(2,70000,70000),(3,80000,75000),
                            (4,90000,95000),(5,100000,95000),(6,110000,105000))

    val df = spark.createDataFrame(sales_target).toDF(schema:_*)
     df.show()

    /*  Create a new column achievement_status based on achieved_amount:
            o 'Exceeded' if achieved_amount > target_amount
            o 'Met' if achieved_amount == target_amount
            o 'Below' if achieved_amount < target_amount
       ===============================================================  */
    val df1 =df.withColumn("Achievement_Status",
                  when($"Achieved_Amount" > $"Target_Amount", "Exceeded")
                    .when($"Achieved_Amount" === $"Target_Amount", "Met")
                    .when($"Achieved_Amount" < $"Target_Amount", "Below"))
    df1.show()

/* Filter records where target_amount is greater than 50000.
===================================================================== */
    val df2 = df.filter($"Target_Amount" > 50000)
    df2.show()

/* Calculate the total count of each achievement_status.
==============================================================*/
    val df3 = df1.groupBy("Achievement_Status").count()
    df3.show()

    spark.stop()
  }
}