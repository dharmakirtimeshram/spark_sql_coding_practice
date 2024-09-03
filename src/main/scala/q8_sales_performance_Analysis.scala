import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q8_sales_performance_Analysis {
  def main(args:Array[String]):Unit={

    val sparkconf =new SparkConf().setAppName("qb8").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val data =List((1,"North-West",12000,"2024-01-10"),(2,"South-East",6000,"2024-01-15"),(3,"East-Central",4000,"2024-02-20"),
                   (4,"West",15000,"2024-02-25"),(5,"North-East",3000,"2024-03-05"),(6,"South-West",7000,"2024-03-12"))
                   .toDF("Sales_Id","Region","Sales_Amount","Sales_Date")
       //data.show()

    /*#Create a new column sales_performance based on sales_amount:
  'Excellent' if sales_amount > 10000
  'Good' if 5000 >= sales_amount <= 10000
  'Average' if sales_amount < 5000
  ===========================================================*/
     val sales_perform = data.withColumn("Sales_Performance", when(($"Sales_AMount")>10000,"Excellent")
                                          .when(($"Sales_AMount")>= 5000 && ($"Sales_AMount")<=10000,"Good")
                                           .when(($"Sales_AMount")<5000,"Average"))
   sales_perform.show()

    /*#Filter sales data where region ends with 'West'.
    ===================================================*/
    val filt =data.filter(($"Region").endsWith("West"))
    filt.show()

    /*#Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount for each performance category.
    ======================================================================================================================*/
    val calc = sales_perform.groupBy("Sales_Performance").agg(sum($"Sales_Amount").alias("Total_Sales_Amount"),
                                                            avg($"Sales_Amount").alias("Average_Sales_Amount"),
                                                           max($"Sales_Amount").alias("Maximum_Sales_Amount"),
                                                           min($"Sales_Amount").alias("Minimum_Sales_Amount"))
    calc.show()


    spark.stop()
  }

}
