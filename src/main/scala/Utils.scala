import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Utils {

  /**
   * Reads a CSV file and returns a DataFrame.
   *
   * @param spark SparkSession
   * @param path Path to the CSV file
   * @return DataFrame
   */
  def readCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header", "true").csv(path)
  }

  /**
   * Writes a DataFrame to JSON files.
   *
   * @param df DataFrame to write
   * @param path Path to the output directory
   */
  def writeJSON(df: DataFrame, path: String): Unit = {
    df.coalesce(1).write.mode(SaveMode.Overwrite).json(path)
  }

  /**
   * Calculates the total number of member months per member.
   *
   * @param memberEligibilityDF DataFrame containing member eligibility data
   * @param memberMonthsDF DataFrame containing member months data
   * @return DataFrame with memberID, fullName, and total member months
   */
  def calculateTotalMemberMonths(memberEligibilityDF: DataFrame, memberMonthsDF: DataFrame): DataFrame = {
    val memberEligibilityRenamedDF = memberEligibilityDF
      .withColumnRenamed("member_id", "member_id_member_eligibility")
    val joinedDF = memberEligibilityRenamedDF
      .join(memberMonthsDF, memberEligibilityRenamedDF.col("member_id_member_eligibility") === memberMonthsDF.col("member_id"))
      .drop("member_id_member_eligibility")
    val memberMonthsCountDF = joinedDF
      .withColumn("fullName",
        when(col("middle_name").isNotNull,
          concat_ws(" ", col("first_name"), col("middle_name"), col("last_name"))
        ).otherwise(
          concat_ws(" ", col("first_name"), col("last_name"))
        ))
      .withColumn("date", col("eligiblity_effective_date"))
      .groupBy("member_id", "fullName").agg(count("date").alias("totalMemberMonths"))
    memberMonthsCountDF
  }

  /**
   * Calculates the total number of member months per member per year.
   *
   * @param memberMonthsDF DataFrame containing member months data
   * @return DataFrame with memberID, year, and total member months per year
   */
  def calculateMemberMonthsPerYear(memberMonthsDF: DataFrame): DataFrame = {
    val memberMonthsWithYearDF = memberMonthsDF
      .withColumnRenamed("eligiblity_effective_date", "date")
      .withColumn("year", year(to_date(col("date"), "yyyy-MM-dd")))

    val memberMonthsPerYearDF = memberMonthsWithYearDF
      .groupBy("member_id", "year").agg(count("date").alias("totalMemberMonths"))
    memberMonthsPerYearDF
  }
}
