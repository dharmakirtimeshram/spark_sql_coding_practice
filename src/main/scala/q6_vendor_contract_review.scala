import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q6_vendor_contract_review {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q6").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Contract_Id","Contract_Amount","Contract_Date")

    val vendor_contract = Seq((1,60000,"2025-11-01"),(2,30000,"2025-11-05"),(3,15000,"2025-11-10"),
                              (4,45000,"2025-11-15"),(5,70000,"2025-11-20"),(6,18000,"2025-11-25"))

    val df = spark.createDataFrame(vendor_contract).toDF(schema:_*)
    df.show()

    /* #Create a new column contract_level based on contract_amount:
           'High' if contract_amount > 50000
           'Medium' if 20000 >= contract_amount <= 50000
           'Low' if contract_amount < 20000
        =========================================================== */
      val contract_level = df.withColumn("Contract_Level",
                                when($"Contract_Amount" > 50000,"High")
                                  .when($"Contract_Amount" >= 20000 && $"Contract_Amount" <= 50000,"Medium")
                                  .when($"Contract_Amount" < 20000,"Low"))
    contract_level.show()

/* #Filter records where contract_date is in 'November 2025'.
===============================================================  */
    val df1 = df.filter($"Contract_Date".between("2025-11-01","2025-11-30"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) contract_amount for each contract_level.
=======================================================================================================================*/
    val df2 = contract_level.groupBy("Contract_Level")
                                .agg(sum($"Contract_Amount").alias("Total_Contract_Amount"),
                                  avg($"Contract_Amount").alias("Average_Contract_Amount"),
                                  max($"Contract_Amount").alias("Maximum_Contract_Amount"),
                                  min($"Contract_Amount").alias("Minimum_Contract_Amount"))
    df2.show()

    spark.stop()
  }
}
