import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q33_sunscription_renewal_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q33").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Subscription_Id","Renewal_Amount","Renewal_Date")

    val subscription_renewal = Seq((1,25000,"2027-11-01"),(2,15000,"2027-11-05"),(3,8000,"2027-11-10"),
                                   (4,17000,"2027-11-15"),(5,30000,"2027-11-20"),(6,5000,"2027-11-25"))

    val df = spark.createDataFrame(subscription_renewal).toDF(schema:_*)
    df.show()

    /* Create a new column renewal_category based on renewal_amount:
          o 'High' if renewal_amount > 20000
          o 'Medium' if 10000 >= renewal_amount <= 20000
          o 'Low' if renewal_amount < 10000
    ================================================== */
     val df1 = df.withColumn("Renewal_Category",
                   when($"Renewal_Amount" > 20000,"High")
                   .when($"Renewal_Amount" >= 10000 && $"Renewal_Amount" <= 20000,"Medium")
                   .when($"Renewal_Amount" < 10000,"Low"))

/* Filter records where renewal_date is in 'November 2027'.
=============================================================== */
    val df2 = df.filter($"Renewal_Date".between("2027-11-01","2027-11-30"))
    df2.show()

/* For each renewal_category, calculate the total (sum), average (avg), maximum (max), and minimum (min) renewal_amount.
=======================================================================================================================*/
    val df3 = df1.groupBy("Renewal_Category")
                   .agg(sum($"Renewal_Amount").alias("Total Renewal Amount"),
                     avg($"Renewal_Amount").alias("Average Renewal Amount"),
                     max($"Renewal_Amount").alias("Maximum Renewal Amount"),
                     min($"Renewal_Amount").alias("Minimum Renewal Amount"))
    df3.show()

spark.stop()
  }
}