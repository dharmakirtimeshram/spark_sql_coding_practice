import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q36_marketing_campaign_expenses {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb36").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Campaign_Id","Campaign_Name","Expense_Amount","Expense_Date")

    val campaign_expenses = Seq((1,"Summer Blast",22000,"2024-11-01"),(2,"Summer Sale",12000,"2024-11-05"),
                                (3,"Winter Campaign",8000,"2024-11-10"),(4,"Summer Special",15000,"2024-11-15"),
                                (5,"Winter Special",9000,"2024-11-20"),(6,"Summer Promo",2500,"2024-11-25"))

       val data = spark.sparkContext.parallelize(campaign_expenses)
    val df = spark.createDataFrame(data).toDF(schema:_*)
    df.show()

    /*  #Create a new column expense_type based on expense_amount:
    'High' if expense_amount > 20000
    'Medium' if 10000 <= expense_amount <= 20000
    'Low' if expense_amount < 10000
   =============================================================== */
    val expense_type = df.withColumn("Expenses_Type",
                           when($"Expense_Amount" > 20000,"High")
                             .when($"Expense_Amount" >= 10000 && $"Expense_Amount" <= 20000,"Medium").
                             when($"Expense_Amount" < 10000,"Low"))
               expense_type.show()

       /* # Filter expenses where campaign_name starts with 'Summer'.
        ============================================================= */

    val df1  = df.filter($"Campaign_Name".contains("Summer"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) expense_amount for each expense_type.
===================================================================================================================*/
    val df2 = expense_type.groupBy("Expenses_Type")
                          .agg(sum($"Expense_Amount").alias("Total Expense"),
                            avg($"Expense_Amount").alias("Average Expense"),
                            max($"Expense_Amount").alias("Maximum Expense"),
                            min($"Expense_Amount").alias("Minimum Expense"))

              df2.show()

     spark.stop()
  }
}
