import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object q14_Shipment_Tracking {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb14").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val data = List((1,"Asia",15000,"2024-01-10"),(2,"Europe",6000,"2024-01-15"),(3,"Asia",3000,"2024-02-20"),
                    (4,"Asia",20000,"2024-02-25"),(5,"North America",4000,"2024-03-05"),(6,"Asia",8000,"2024-03-05"))
                    .toDF("Shipment_Id","Destination","Shipment_Value","Shipment_Date")
   // data.show()

    /*#Create a new column value_category based on shipment_value:
  'High' if shipment_value > 10000
  'Medium' if 5000 >= shipment_value <= 10000
  'Low' if shipment_value < 5000
  ==============================================*/
    val value_category = data.withColumn("Value_Category",
                                  when(($"Shipment_value")>10000,"High")
                                 .when(($"Shipment_value")>= 5000 && ($"Shipment_value")<= 10000,"Medium")
                                  .when(($"Shipment_value")<5000,"Low"))
    //value_category.show()

/*# Filter shipments where destination contains 'Asia'.
=========================================================*/
    val filt = data.filter(($"Destination").contains("Asia"))
    filt.show()

/*# Calculate the total (sum), average (avg), maximum (max), and minimum (min) shipment_value for each value_category.
======================================================================================================================*/

    val calc = value_category.groupBy("Value_Category")
                                    .agg(sum($"Shipment_Value").alias("Total Shipment Value"),
                                    avg($"Shipment_Value").alias("Average Shipment Value"),
                                    max($"Shipment_Value").alias(" Maximum Shipment Value"),
                                      min($"Shipment_Value").alias("Minimum Shipment Value"))
    calc.show()

    spark.stop()
  }

}

