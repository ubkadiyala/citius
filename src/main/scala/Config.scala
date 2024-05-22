import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

object Config {
  private val config: Config = ConfigFactory.parseFile(new File("src/main/resources/application.conf"))

  val memberEligibilityPath: String = config.getString("input.memberEligibility")
  val memberMonthsPath: String = config.getString("input.memberMonths")

  val totalMemberMonthsOutputPath: String = config.getString("output.totalMemberMonths")
  val memberMonthsPerYearOutputPath: String = config.getString("output.memberMonthsPerYear")
}
