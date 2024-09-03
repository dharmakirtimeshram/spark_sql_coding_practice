import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q16_sales_target_achievement {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb16").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val sales_data = List((1,"John Smith",15000,12000),(2,"Jane Doe",9000,10000),(3,"John Doe",5000,6000),
                        (4,"John Smith",13000,6000),(5,"John Doe",7000,7000),(6,"John DOe",8000,8500))
                         .toDF("Sales_Id","Sales_Rep","Sales_Amount","Target_Amount")
    sales_data.show()

    /*#Create a new column achievement_status based on the comparison between sales_amount and target_amount:
             'Above Target' if sales_amount >= target_amount
             'Below Target' if sales_amount < target_amount
 ======================================================*/

    val achievement = sales_data.withColumn("Achievement Status",
                                when($"Sales_Amount" >= $"Target_Amount","Above Target")
                               .when($"Sales_Amount" < $"Target_Amount"," Below Target"))
      achievement.show()

    /* #Filter sales records where sales_rep contains 'John'.
    =========================================================*/

    val filt = sales_data.filter(($"Sales_Rep").contains("John"))
          filt.show

    /* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount for each achievement_status.
    =========================================================================================================================*/

    val calc = achievement.groupBy("Achievement Status")
                               .agg(sum($"Sales_Amount").alias("Total Sales Amount"),
                                 avg($"Sales_Amount").alias("Average Sales Amount"),
                                 max($"Sales_Amount").alias("Maximum Sales Amount"),
                                 min($"Sales_Amount").alias("Minimum Sales Amount"))

       calc.show()

    spark.stop()
  }
}