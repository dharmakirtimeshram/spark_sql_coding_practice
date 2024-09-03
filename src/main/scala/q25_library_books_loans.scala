import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q25_library_books_loans {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb25").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._
    val book_loan = List((1,"History of Rome",40,"2024-01-10"),(2,"Modern History",20,"2024-01-15"),
                         (3,"Ancient History",10,"2024-02-20"),(4,"European history",15,"2024-02-25"),
                          (5,"World History",5,"2024-03-05"),(6,"History of Art",35,"2024-03-12"))
                        .toDF("Load_Id","Book_Title","Loan_Duration_Days","Loan_date")
    book_loan.show()

    /*  #Create a new column loan_category based on loan_duration_days:
           'Long-Term' if loan_duration_days > 30
           'Medium-Term' if 15 >= loan_duration_days <= 30
           'Short-Term' if loan_duration_days < 15
    =========================================================     */

    val loan_category = book_loan.withColumn("Loan_Category",
                                  when($"Loan_Duration_Days" > 30,"Long_Term")
                                    .when($"Loan_Duration_Days" >= 15 && $"Loan_Duration_Days" <= 30,"Medium_Term")
                                    .when($"Loan_Duration_Days" < 15,"Short_Term"))
      loan_category.show()

/* #Filter loans where book_title contains 'History'.
======================================================  */
    val filt_df = book_loan.filter($"Book_Title".contains("History"))
    filt_df.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) loan_duration_days for each loan_category.
=========================================================================================================================*/

    val calc_df = loan_category.groupBy("Loan_Category")
                               .agg(sum($"Loan_Duration_Days").alias("Total Loan Duration Days"),
                                 avg($"Loan_Duration_Days").alias("Average Loan Duration Days"),
                                 max($"Loan_Duration_Days").alias("Maximum Loan Duration Days"),
                                 min($"Loan_Duration_Days").alias("Minimum Loan Duration Days"))
    calc_df.show()


  spark.stop()
  }
}
