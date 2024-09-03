import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q28_event_sponsorship {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb28").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val event_sponsorship =List((1,"Alpha Corp",12000,"2024-02-05"),(2,"Beta LLC",7000,"2024-02-15"),
                               (3,"Gamma Inc",3000,"2024-02-15"),(4," Delta Ltd",9000,"2024-02-20"),
                                (5,"Epsilon Co",15000,"2024-02-20"),(6,"Zeta AG",4000,"2024-02-28"))
                              .toDF("Sponsor_Id","Sponsor_Name","Sponsorship_Amount","Sponsorship_Date")
     event_sponsorship.show()

    /* #Create a new column amount_category based on sponsorship_amount:
    'High' if sponsorship_amount > 10000
    'Medium' if 5000 >= sponsorship_amount <= 10000
    'Low' if sponsorship_amount < 5000
   ============================================================*/
    val amount_category = event_sponsorship.withColumn("Amount_Category",
                                       when($"Sponsorship_Amount" > 10000,"High")
                                         .when($"Sponsorship_Amount" >= 5000 && $"Sponsorship_Amount" <= 10000,"Medium")
                                         .when($"Sponsorship_Amount" < 5000,"Low"))
    amount_category.show()


/* #Filter sponsorships where sponsorship_date is in 'February 2024'.
================================================================== */
    val filt_df = event_sponsorship.filter($"Sponsorship_Date".between("2024-02-01","2024-02-29"))
    filt_df.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) sponsorship_amount for each amount_category.
===========================================================================================================================*/
    val calc_df = amount_category.groupBy("Amount_Category")
                                  .agg(sum($"Sponsorship_Amount").alias("Total Sponsorship_Amount"),
                                    avg($"Sponsorship_Amount").alias("Average Sponsorship_Amount"),
                                    max($"Sponsorship_Amount").alias("Maximum Sponsorship_Amount"),
                                    min($"Sponsorship_Amount").alias("Minimum Sponsorship_Amount"))
    calc_df.show()
    spark.stop()
  }
}
