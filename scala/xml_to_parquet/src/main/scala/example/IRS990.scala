/**
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates. All rights reserved.
 */
package example

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

case class Extract(
  EIN: Option[Long], BusinessName: Option[String], TaxYear: Option[Long], ReturnType: Option[String],
  AuditedFinancialStatements: Option[Boolean], BalanceSheetAmountsReported: Option[Boolean],
  BenefitsPaidToMembersCY: Option[Long], CollectionsOfArt: Option[Boolean], ConservationEasements: Option[Boolean],
  ContributionsGrantsCurrentYear: Option[Long], ContributionsGrantsPriorYear: Option[Long], CreditCounseling: Option[Boolean],
  DonorAdvisedFunds: Option[Boolean], ForeignActivities: Option[Boolean], ForeignOffice: Option[Boolean],
  FundraisingActivities: Option[Boolean], Gaming: Option[Boolean], GrantsAndSimilarAmntsCY: Option[Long],
  GrantsToIndividuals: Option[Boolean], GrantsToOrganizations: Option[Boolean], Hospital: Option[Boolean],
  InvestmentIncomeCurrentYear: Option[Long], MembershipDues: Option[Long], MoreThan5000KToIndividuals: Option[Boolean],
  MoreThan5000KToOrganizations: Option[Boolean], NetAssetsOrFundBalancesBOY: Option[Long],
  NetAssetsOrFundBalancesEOY: Option[Long], NetUnrelatedBusinessTxblIncome: Option[Long], OtherExpensePriorYear: Option[Long],
  OtherExpensesCurrentYear: Option[Long], OtherRevenueCurrentYear: Option[Long], OtherRevenuePriorYear: Option[Long],
  PoliticalActivities: Option[Boolean], ProfessionalFundraising: Option[Boolean], ProgramServiceRevenueCY: Option[Long],
  ProgramServiceRevenuePriorYear: Option[Long], Revenue: Option[Long], RevenuesLessExpensesCY: Option[Long],
  RevenuesLessExpensesPriorYear: Option[Long], SalariesEtcCurrentYear: Option[Long], SalariesEtcPriorYear: Option[Long],
  School: Option[Boolean], SubjectToProxyTax: Option[Boolean], TermOrPermanentEndowments: Option[Boolean],
  TotalAssetsBOY: Option[Long], TotalAssetsEOY: Option[Long], TotalExpenses: Option[Long],
  TotalExpensesCurrentYear: Option[Long], TotalExpensesPriorYear: Option[Long], TotalFundrsngExpCurrentYear: Option[Long],
  TotalGrossUBI: Option[Long], TotalLiabilitiesBOY: Option[Long], TotalLiabilitiesEOY: Option[Long],
  TotalNbrEmployees: Option[Long], TotalNbrVolunteers: Option[Long], TotalProfFundrsngExpCY: Option[Long],
  TotalRevenueCurrentYear: Option[Long], TotalRevenuePriorYear: Option[Long],

  PFContriReceivedRevAndExpnss: Option[Long], PFInterestOnSavingsRevAndExpnss: Option[Long],
  PFInterestOnSavingsNetInvstIncm: Option[Long], PFDividendsRevAndExpnss: Option[Long],
  PFDividendsNetInvstIncm: Option[Long], PFCapitalGainNetIncmNetInvstIncm: Option[Long],
  PFTotalRevenueAndExpenses: Option[Long], PFTotalNetInvestmentIncome: Option[Long],
  PFCompOfcrDirTrstRevAndExpnss: Option[Long], PFCompOfcrDirTrstNetInvstIncm: Option[Long],
  PFCompOfcrDirTrstDsbrsChrtblPrps: Option[Long], PFAccountingFeesRevAndExpnss: Option[Long],
  PFAccountingFeesNetInvstIncm: Option[Long], PFAccountingFeesDsbrsChrtblPrps: Option[Long],
  PFTaxesRevAndExpnss: Option[Long], PFTaxesNetInvstIncm: Option[Long], PFTaxesDsbrsChrtblPrps: Option[Long],
  PFOtherExpensesRevAndExpnss: Option[Long], PFOtherExpensesNetInvstIncm: Option[Long],
  PFOtherExpensesDsbrsChrtblPrps: Option[Long], PFTotOprExpensesRevAndExpnss: Option[Long],
  PFTotOprExpensesNetInvstIncm: Option[Long], PFTotOprExpensesDsbrsChrtblPrps: Option[Long],
  PFContriGiftsPaidRevAndExpnss: Option[Long], PFContriGiftsPaidDsbrsChrtblPrps: Option[Long],
  PFTotalExpensesRevAndExpnss: Option[Long], PFTotalExpensesNetInvstIncm: Option[Long],
  PFTotalExpensesDsbrsChrtblPrps: Option[Long], PFExcessOfRevenueOverExpenses: Option[Long],
  PFNetInvestmentIncome: Option[Long],

  PFCashBOY: Option[Long], PFCashEOY: Option[Long], PFCashEOYFMV: Option[Long],
  PFInvestmentsOtherBOY: Option[Long], PFInvestmentsOtherEOY: Option[Long],
  PFInvestmentsOtherEOYFMV: Option[Long], PFTotalAssetsBOY: Option[Long],
  PFTotalAssetsEOY: Option[Long], PFTotalAssetsEOYFMV: Option[Long],
  PFTotalLiabilitiesBOY: Option[Long], PFTotalLiabilitiesEOY: Option[Long],
  PFOrgThatDoNotFollowSFAS117: Option[String], PFCapitalStockBOY: Option[Long],
  PFCapitalStockEOY: Option[Long], PFPaidInOrCapitalSurplusBOY: Option[Long],
  PFPaidInOrCapitalSurplusEOY: Option[Long], PFRetainedEarningsBOY: Option[Long],
  PFRetainedEarningsEOY: Option[Long], PFTotalNetAssetsBOY: Option[Long],
  PFTotalNetAssetsEOY: Option[Long], PFTotalLiabilitiesNetAssetsBOY: Option[Long],
  PFTotalLiabilitiesNetAssetsEOY: Option[Long]
)

object IRS990 {
  def toBooleanCustom(text : Option[String]) : Option[Boolean] = {
    if (text == Some("true") || text == Some("1")) {
      Some(true)
    } else if (text == Some("false") || text == Some("0")) {
      Some(false)
    } else {
      None
    }
  }

  def extractDataNew(text : String) : scala.collection.mutable.Map[String, Option[String]] = {
    var data = scala.collection.mutable.Map[String, Option[String]]()
    val doc = scala.xml.XML.loadString(text.replaceFirst("^([\\W]+)<","<"))

    val returnVersion = (doc \ "@returnVersion").text
    val returnYear = returnVersion.take(4).toInt

    // The header data.
    val returnHeader = doc \ "ReturnHeader"
    val filer = returnHeader \ "Filer"
    val name = filer \ "Name"
    var returnType = ""
    if (returnYear < 2013) {
      returnType = (returnHeader \ "ReturnType").text
      data += ("EIN" -> Some((filer \ "EIN").text))
      data += ("BusinessName" -> Some((name \ "BusinessNameLine1").text))
      data += ("TaxYear" -> Some((returnHeader \ "TaxYear").text))
      data += ("ReturnType" -> Some(returnType))
    } else {
      returnType = (returnHeader \ "ReturnTypeCd").text
      data += ("EIN" -> Some((filer \ "EIN").text))
      data += ("BusinessName" -> None)
      data += ("TaxYear" -> Some((returnHeader \ "TaxYr").text))
      data += ("ReturnType" -> Some(returnType))
    }

    // The return data.
    val path = "IRS" + returnType
    val returnData = doc \ "ReturnData" \ path

    // Fields for 990 and 990EZ.
    val fields = List("AuditedFinancialStatements", "BalanceSheetAmountsReported", "BenefitsPaidToMembersCY",
        "CollectionsOfArt", "ConservationEasements", "ContributionsGrantsCurrentYear",
        "ContributionsGrantsPriorYear", "CreditCounseling", "DonorAdvisedFunds", "ForeignActivities",
        "ForeignOffice", "FundraisingActivities", "Gaming", "GrantsAndSimilarAmntsCY", "GrantsToIndividuals",
        "GrantsToOrganizations", "Hospital", "InvestmentIncomeCurrentYear", "MembershipDues",
        "MoreThan5000KToIndividuals", "MoreThan5000KToOrganizations", "NetAssetsOrFundBalancesBOY",
        "NetAssetsOrFundBalancesEOY", "NetUnrelatedBusinessTxblIncome", "OtherExpensePriorYear",
        "OtherExpensesCurrentYear", "OtherRevenueCurrentYear", "OtherRevenuePriorYear", "PoliticalActivities",
        "ProfessionalFundraising", "ProgramServiceRevenueCY", "ProgramServiceRevenuePriorYear", "Revenue",
        "RevenuesLessExpensesCY", "RevenuesLessExpensesPriorYear", "SalariesEtcCurrentYear",
        "SalariesEtcPriorYear", "School", "SubjectToProxyTax", "TermOrPermanentEndowments", "TotalAssetsBOY",
        "TotalAssetsEOY", "TotalExpenses", "TotalExpensesCurrentYear", "TotalExpensesPriorYear",
        "TotalFundrsngExpCurrentYear", "TotalGrossUBI", "TotalLiabilitiesBOY", "TotalLiabilitiesEOY",
        "TotalNbrEmployees", "TotalNbrVolunteers", "TotalProfFundrsngExpCY",
        "TotalRevenueCurrentYear", "TotalRevenuePriorYear"
    )
    for (key <- fields) {
      val value = (returnData \ key).text
      if (value == "") {
        data += (key -> None)
      } else {
        data += (key -> Some(value))
      }
    }

    // Fields for 990PF.
    val revenue = returnData \ "AnalysisOfRevenueAndExpenses"
    val balance = returnData \ "BalanceSheets"
    val pf_fields_revenue = List(
      "ContriReceivedRevAndExpnss", "InterestOnSavingsRevAndExpnss",
      "InterestOnSavingsNetInvstIncm", "DividendsRevAndExpnss", "DividendsNetInvstIncm",
      "CapitalGainNetIncmNetInvstIncm", "TotalRevenueAndExpenses", "TotalNetInvestmentIncome",
      "CompOfcrDirTrstRevAndExpnss", "CompOfcrDirTrstNetInvstIncm", "CompOfcrDirTrstDsbrsChrtblPrps",
      "AccountingFeesRevAndExpnss", "AccountingFeesNetInvstIncm", "AccountingFeesDsbrsChrtblPrps",
      "TaxesRevAndExpnss", "TaxesNetInvstIncm", "TaxesDsbrsChrtblPrps", "OtherExpensesRevAndExpnss",
      "OtherExpensesNetInvstIncm", "OtherExpensesDsbrsChrtblPrps", "TotOprExpensesRevAndExpnss",
      "TotOprExpensesNetInvstIncm", "TotOprExpensesDsbrsChrtblPrps", "ContriGiftsPaidRevAndExpnss",
      "ContriGiftsPaidDsbrsChrtblPrps", "TotalExpensesRevAndExpnss", "TotalExpensesNetInvstIncm",
      "TotalExpensesDsbrsChrtblPrps", "ExcessOfRevenueOverExpenses", "NetInvestmentIncome"
    )
    val pf_fields_balance = List(
      "CashBOY", "CashEOY", "CashEOYFMV", "InvestmentsOtherBOY", "InvestmentsOtherEOY",
      "InvestmentsOtherEOYFMV", "TotalAssetsBOY", "TotalAssetsEOY", "TotalAssetsEOYFMV",
      "TotalLiabilitiesBOY", "TotalLiabilitiesEOY", "OrgThatDoNotFollowSFAS117", "CapitalStockBOY",
      "CapitalStockEOY", "PaidInOrCapitalSurplusBOY", "PaidInOrCapitalSurplusEOY",
      "RetainedEarningsBOY", "RetainedEarningsEOY", "TotalNetAssetsBOY", "TotalNetAssetsEOY",
      "TotalLiabilitiesNetAssetsBOY", "TotalLiabilitiesNetAssetsEOY"
    )
    for (key <- pf_fields_revenue) {
      val pf_key = "PF" + key
      var value = ""
      if (returnType == "990PF") {
        value = (revenue \ key).text
      }
      if (value == "") {
        data += (pf_key -> None)
      } else {
        data += (pf_key -> Some(value))
      }
    }
    for (key <- pf_fields_balance) {
      val pf_key = "PF" + key
      var value = ""
      if (returnType == "990PF") {
        value = (balance \ key).text
      }
      if (value == "") {
        data += (pf_key -> None)
      } else {
        data += (pf_key -> Some(value))
      }
    }

    data
  }

  def main(args: Array[String]) {
    val spark = SparkSession
        .builder()
        .appName("IRS990")
        .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val sqlContext = new SQLContext(spark.sparkContext)
    val files = spark.sparkContext.wholeTextFiles(args(0), 100)

    // Run the extraction.
    val processed = sqlContext.createDataFrame(files.map( r => {
      val data = extractDataNew(r._2)
      Extract(
        data("EIN").map(_.toLong), data("BusinessName"),
        data("TaxYear").map(_.toLong), data("ReturnType"),
        toBooleanCustom(data("AuditedFinancialStatements")), toBooleanCustom(data("BalanceSheetAmountsReported")),
        data("BenefitsPaidToMembersCY").map(_.toLong), toBooleanCustom(data("CollectionsOfArt")),
        toBooleanCustom(data("ConservationEasements")), data("ContributionsGrantsCurrentYear").map(_.toLong),
        data("ContributionsGrantsPriorYear").map(_.toLong), toBooleanCustom(data("CreditCounseling")),
        toBooleanCustom(data("DonorAdvisedFunds")), toBooleanCustom(data("ForeignActivities")),
        toBooleanCustom(data("ForeignOffice")), toBooleanCustom(data("FundraisingActivities")),
        toBooleanCustom(data("Gaming")), data("GrantsAndSimilarAmntsCY").map(_.toLong),
        toBooleanCustom(data("GrantsToIndividuals")), toBooleanCustom(data("GrantsToOrganizations")),
        toBooleanCustom(data("Hospital")), data("InvestmentIncomeCurrentYear").map(_.toLong),
        data("MembershipDues").map(_.toLong), toBooleanCustom(data("MoreThan5000KToIndividuals")),
        toBooleanCustom(data("MoreThan5000KToOrganizations")), data("NetAssetsOrFundBalancesBOY").map(_.toLong),
        data("NetAssetsOrFundBalancesEOY").map(_.toLong), data("NetUnrelatedBusinessTxblIncome").map(_.toLong),
        data("OtherExpensePriorYear").map(_.toLong), data("OtherExpensesCurrentYear").map(_.toLong),
        data("OtherRevenueCurrentYear").map(_.toLong), data("OtherRevenuePriorYear").map(_.toLong),
        toBooleanCustom(data("PoliticalActivities")), toBooleanCustom(data("ProfessionalFundraising")),
        data("ProgramServiceRevenueCY").map(_.toLong), data("ProgramServiceRevenuePriorYear").map(_.toLong),
        data("Revenue").map(_.toLong), data("RevenuesLessExpensesCY").map(_.toLong),
        data("RevenuesLessExpensesPriorYear").map(_.toLong), data("SalariesEtcCurrentYear").map(_.toLong),
        data("SalariesEtcPriorYear").map(_.toLong), toBooleanCustom(data("School")),
        toBooleanCustom(data("SubjectToProxyTax")), toBooleanCustom(data("TermOrPermanentEndowments")),
        data("TotalAssetsBOY").map(_.toLong), data("TotalAssetsEOY").map(_.toLong),
        data("TotalExpenses").map(_.toLong), data("TotalExpensesCurrentYear").map(_.toLong),
        data("TotalExpensesPriorYear").map(_.toLong), data("TotalFundrsngExpCurrentYear").map(_.toLong),
        data("TotalGrossUBI").map(_.toLong), data("TotalLiabilitiesBOY").map(_.toLong),
        data("TotalLiabilitiesEOY").map(_.toLong), data("TotalNbrEmployees").map(_.toLong),
        data("TotalNbrVolunteers").map(_.toLong), data("TotalProfFundrsngExpCY").map(_.toLong),
        data("TotalRevenueCurrentYear").map(_.toLong), data("TotalRevenuePriorYear").map(_.toLong),

        data("PFContriReceivedRevAndExpnss").map(_.toLong),
        data("PFInterestOnSavingsRevAndExpnss").map(_.toLong),
        data("PFInterestOnSavingsNetInvstIncm").map(_.toLong),
        data("PFDividendsRevAndExpnss").map(_.toLong),
        data("PFDividendsNetInvstIncm").map(_.toLong),
        data("PFCapitalGainNetIncmNetInvstIncm").map(_.toLong),
        data("PFTotalRevenueAndExpenses").map(_.toLong),
        data("PFTotalNetInvestmentIncome").map(_.toLong),
        data("PFCompOfcrDirTrstRevAndExpnss").map(_.toLong),
        data("PFCompOfcrDirTrstNetInvstIncm").map(_.toLong),
        data("PFCompOfcrDirTrstDsbrsChrtblPrps").map(_.toLong),
        data("PFAccountingFeesRevAndExpnss").map(_.toLong),
        data("PFAccountingFeesNetInvstIncm").map(_.toLong),
        data("PFAccountingFeesDsbrsChrtblPrps").map(_.toLong),
        data("PFTaxesRevAndExpnss").map(_.toLong),
        data("PFTaxesNetInvstIncm").map(_.toLong),
        data("PFTaxesDsbrsChrtblPrps").map(_.toLong),
        data("PFOtherExpensesRevAndExpnss").map(_.toLong),
        data("PFOtherExpensesNetInvstIncm").map(_.toLong),
        data("PFOtherExpensesDsbrsChrtblPrps").map(_.toLong),
        data("PFTotOprExpensesRevAndExpnss").map(_.toLong),
        data("PFTotOprExpensesNetInvstIncm").map(_.toLong),
        data("PFTotOprExpensesDsbrsChrtblPrps").map(_.toLong),
        data("PFContriGiftsPaidRevAndExpnss").map(_.toLong),
        data("PFContriGiftsPaidDsbrsChrtblPrps").map(_.toLong),
        data("PFTotalExpensesRevAndExpnss").map(_.toLong),
        data("PFTotalExpensesNetInvstIncm").map(_.toLong),
        data("PFTotalExpensesDsbrsChrtblPrps").map(_.toLong),
        data("PFExcessOfRevenueOverExpenses").map(_.toLong),
        data("PFNetInvestmentIncome").map(_.toLong),

        data("PFCashBOY").map(_.toLong),
        data("PFCashEOY").map(_.toLong),
        data("PFCashEOYFMV").map(_.toLong),
        data("PFInvestmentsOtherBOY").map(_.toLong),
        data("PFInvestmentsOtherEOY").map(_.toLong),
        data("PFInvestmentsOtherEOYFMV").map(_.toLong),
        data("PFTotalAssetsBOY").map(_.toLong),
        data("PFTotalAssetsEOY").map(_.toLong),
        data("PFTotalAssetsEOYFMV").map(_.toLong),
        data("PFTotalLiabilitiesBOY").map(_.toLong),
        data("PFTotalLiabilitiesEOY").map(_.toLong),
        data("PFOrgThatDoNotFollowSFAS117"),
        data("PFCapitalStockBOY").map(_.toLong),
        data("PFCapitalStockEOY").map(_.toLong),
        data("PFPaidInOrCapitalSurplusBOY").map(_.toLong),
        data("PFPaidInOrCapitalSurplusEOY").map(_.toLong),
        data("PFRetainedEarningsBOY").map(_.toLong),
        data("PFRetainedEarningsEOY").map(_.toLong),
        data("PFTotalNetAssetsBOY").map(_.toLong),
        data("PFTotalNetAssetsEOY").map(_.toLong),
        data("PFTotalLiabilitiesNetAssetsBOY").map(_.toLong),
        data("PFTotalLiabilitiesNetAssetsEOY").map(_.toLong)
      )
    }))

    // Persist the extract to disk.
    var to_df = processed.toDF()
    to_df.write.parquet(args(1))

    // Determine the number of output records.
    val saved_df = spark.read.parquet(args(1))
    println("Total records produced = " + saved_df.count())

    spark.stop()
  }
}
