import org.apache.spark.sql.SparkSession

object MemberEligibility {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Member Eligibility")
      .master("local[*]")
      .getOrCreate()

    // Read the datasets
    val memberEligibilityDF = Utils.readCSV(spark, Config.memberEligibilityPath)
    val memberMonthsDF = Utils.readCSV(spark, Config.memberMonthsPath)

    // Task 1: Calculate total number of member months per member
    val totalMemberMonthsDF = Utils.calculateTotalMemberMonths(memberEligibilityDF, memberMonthsDF)
    Utils.writeJSON(totalMemberMonthsDF, Config.totalMemberMonthsOutputPath)

    // Task 2: Calculate total number of member months per member per year
    val memberMonthsPerYearDF = Utils.calculateMemberMonthsPerYear(memberMonthsDF)
    Utils.writeJSON(memberMonthsPerYearDF, Config.memberMonthsPerYearOutputPath)

    spark.stop()
  }
}
