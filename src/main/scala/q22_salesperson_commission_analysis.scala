import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q22_salesperson_commission_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q22").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("salesperson_Id","Sales_Amount","Commission_Date")

    val sales_commission = Seq((1,55000,"2027-02-01"),(2,30000,"2027-02-05"),(3,15000,"2027-02-10"),
                               (4,45000,"2027-02-15"),(5,60000,"20-02-20"),(6,18000,"2027-02-25"))

    val df = spark.createDataFrame(sales_commission).toDF(schema:_*)
    df.show()


/* Create a new column commission_band based on sales_amount:
        o 'Top Performer' if sales_amount > 50000
        o 'Achiever' if 20000 >= sales_amount <= 50000
        o 'Contributor' if sales_amount < 20000
   ========================================================== */
    val commission_band = df.withColumn("Commission_Band",
                             when($"Sales_Amount" > 50000,"Top Performer")
                               .when($"Sales_Amount" >= 20000 && $"Sales_Amount" <= 50000,"Achiever")
                               .when($"Sales_Amount" < 20000,"Contributor"))
    commission_band.show()

/*  Filter records where commission_date is in 'February 2027'.
================================================================== */
    val df1 = df.filter($"Commission_Date".between("2027-02-01","2027-02-28"))
    df1.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount for each commission_band.
=====================================================================================================================*/

    val df2 =  commission_band.groupBy("Commission_Band")
                               .agg(sum($"Sales_Amount").alias("Total_Sales_Amount"),
                                 avg($"Sales_Amount").alias("Average_Sales_Amount"),
                                 max($"Sales_Amount").alias("Maximum_Sales_Amount"),
                                 min($"Sales_Amount").alias("Minimum_Sales_Amount"))
   df2.show()

 spark.stop()
  }
}
