import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q1_sales_target_achievement {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf().setAppName("q1").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("SalesPerson_Id","Sales_Target","Sales_Achieved","Achievement_Date")

    val sales_target = Seq((1,20000,22000,"2025-07-01"),(2,15000,15000,"2025-07-05"),(3,30000,28000,"2025-07-10"),
                           (4,18000,19000,"2025-07-15"),(5,25000,26000,"2025-07-20"),(6,22000,21000,"2025-07-25"))
    val df = spark.createDataFrame(sales_target).toDF(schema:_*)

    df.show()

    /* #Create a new column achievement_status based on sales_achieved:
       'Exceeds Target' if sales_achieved > sales_target
       'Meets Target' if sales_achieved == sales_target
       'Below Target' if sales_achieved < sales_target
       ======================================================  */
    val achievement_status =df.withColumn("Achievement_Status",
                                when($"Sales_Achieved" > $"Sales_Target" , "Exceeds Target")
                                  .when($"Sales_Achieved" === $"Sales_Target" , "Meets Target")
                                  .when($"Sales_Achieved" < $"Sales_Target" , "Below Target"))
    achievement_status.show()

/* # Filter records where achievement_date is in 'July 2025'.
=============================================================== */
    val df1 = df.filter($"Achievement_Date".between("2025-07-01","2025-07-31"))
    df1.show()

/* #Calculate the count (count) of each achievement_status.
==============================================================*/
    val df2 = achievement_status.groupBy("Achievement_Status").count()
    df2.show()


   spark.stop()
  }

}



