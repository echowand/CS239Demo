/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "bible+shakes.nopunc" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2)

    // bigramCount: RDD[String, int]
    val bigramCount = logData.map(line => line.split(" ").sliding(2)).
      flatMap {
        identity
      }.map {
      _.mkString(" ")
    }.map(word => (word, 1)).reduceByKey(_ + _)

    // wordsCount: RDD[String, Array[(String, Count)]]
    val wordsCount = bigramCount.map(x => (x._1, (x._1, x._2))).flatMap(x => {
      val words = x._1.split(" ")
      words.map(word => (word, x._2))
    }
    ).groupByKey()

    // wordsTopCount: RDD[String, Array[(String, Count)]]
    val wordsTopCount = wordsCount.map({ case (key, iter) =>
      (key, iter.toSeq.sortBy(x => -x._2).take(5))
    })

    wordsTopCount.foreach(x => println(x))

    wordsTopCount.saveAsTextFile("outputdir")
  }
}

object FECSQLApp {

  import org.apache.spark.sql.SparkSession

  import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
  import org.apache.spark.sql.Encoder


  case class CM(CMTE_ID: String, CMTE_NM: String, TRES_NM: String,
                CMTE_ST1: String, CMTE_ST2: String, CMTE_CITY: String,
                CMTE_ST: String, CMTE_ZIP: String, CMTE_DSGN: String,
                CMTE_TP: String, CMTE_PTY_AFFILIATION: String, CMTE_FILING_FREQ: String,
                ORG_TP: String, CONNECTED_ORG_NM: String, CAND_ID: String)

  case class CN(CAND_ID: String, CAND_NAME: String, CAND_PTY_AFFILIATION: String, CAND_ELECTION_YR: Int,
                CAND_OFFICE_ST: String, CAND_OFFICE: String, CAND_OFFICE_DISTRICT: String, CAND_ICI: String,
                CAND_STATUS: String, CAND_PCC: String, CAND_ST1: String, CAND_ST2: String, CAND_CITY: String,
                CAND_ST: String, CAND_ZIP: String)

  case class CCL(CAND_ID: String, CAND_ELECTION_YR: Int, FEC_ELECTION_YR: Int,
                 CMTE_ID: String, CMTE_TP: String, CMTE_DSGN: String, LINKAGE_ID: String)

  case class ITCONT(CMTE_ID: String, AMNDT_IND: String, RPT_TP: String, TRANSACTION_PGI: String,
                    IMAGE_NUM: String, TRANSACTION_TP: String, ENTITY_TP: String, NAME: String,
                    CITY: String, STATE: String, ZIP_CODE: String, EMPLOYER: String,
                    OCCUPATION: String, TRANSACTION_DT: String, TRANSACTION_AMT: Float,
                    OTHER_ID: String, TRAN_ID: String, FILE_NUM: String, MEMO_CD: String,
                    MEMO_TEXT: String, SUB_ID: String)

  case class ITPAS(CMTE_ID: String, AMNDT_IND: String, RPT_TP: String, TRANSACTION_PGI: String,
                   IMAGE_NUM: String, TRANSACTION_TP: String, ENTITY_TP: String, NAME: String,
                   CITY: String, STATE: String, ZIP_CODE: String, EMPLOYER: String,
                   OCCUPATION: String, TRANSACTION_DT: String, TRANSACTION_AMT: Float,
                   OTHER_ID: String, CAND_ID: String, TRAN_ID: String, FILE_NUM: String,
                   MEMO_CD: String, MEMO_TEXT: String, SUB_ID: String)

  case class ITOTH(CMTE_ID: String, AMNDT_IND: String, RPT_TP: String, TRANSACTION_PGI: String,
                   IMAGE_NUM: String, TRANSACTION_TP: String, ENTITY_TP: String,
                   NAME: String, CITY: String, STATE: String, ZIP_CODE: String, EMPLOYER: String,
                   OCCUPATION: String, TRANSACTION_DT: String, TRANSACTION_AMT: Float,
                   OTHER_ID: String, TRAN_ID: String, FILE_NUM: String, MEMO_CD: String,
                   MEMO_TEXT: String, SUB_ID: String)

  def main(args: Array[String]): Unit = {

    if (args.size != 1) {
      println("FEC directory path required arg missing!")
      System.exit(1)
    }

    val fecDir = args(0)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    val cmDF = spark.sparkContext
      .textFile(fecDir + "/cm.txt")
      .map(_.split("\\|", -1))
      .map(a => CM(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9), a(10), a(11), a(12), a(13), a(14)))
      .toDF()

    val cnDF = spark.sparkContext
      .textFile(fecDir + "/cn.txt")
      .map(_.split("\\|", -1))
      .map(a => CN(a(0), a(1), a(2), a(3).trim.toInt, a(4), a(5), a(6), a(7), a(8), a(9), a(10), a(11), a(12), a(13), a(14)))
      .toDF()

    val cclDF = spark.sparkContext
      .textFile(fecDir + "/ccl.txt")
      .map(_.split("\\|", -1))
      .map(a => CCL(a(0), a(1).trim.toInt, a(2).trim.toInt, a(3), a(4), a(5), a(6)))
      .toDF()

    val itcontDF = spark.sparkContext
      .textFile(fecDir + "/itcont.txt")
      .map(_.split("\\|", -1))
      .map(a => ITCONT(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9), a(10), a(11), a(12), a(13),
        a(14).trim.toFloat, a(15), a(16), a(17), a(18), a(19), a(20)))
      .toDF()

    val itpasDF = spark.sparkContext
      .textFile(fecDir + "/itpas2.txt")
      .map(_.split("\\|", -1))
      .map(a => ITPAS(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9), a(10), a(11), a(12), a(13),
        a(14).trim.toFloat, a(15), a(16), a(17), a(18), a(19), a(20), a(21)))
      .toDF()

    val itothDF = spark.sparkContext
      .textFile(fecDir + "/itoth.txt")
      .map(_.split("\\|", -1))
      .map(a => ITOTH(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9), a(10), a(11), a(12), a(13),
        a(14).trim.toFloat, a(15), a(16), a(17), a(18), a(19), a(20)))
      .toDF()

    cmDF.createOrReplaceTempView("cm")
    cnDF.createOrReplaceTempView("cn")
    cclDF.createOrReplaceTempView("ccl")
    itcontDF.createOrReplaceTempView("itcont")
    itpasDF.createOrReplaceTempView("itpas")
    itothDF.createOrReplaceTempView("itothDF")

    //Q1
    val candidates = spark.sql("SELECT CAND_ID, CAND_PCC, CAND_NAME FROM cn " +
      "WHERE (UPPER(CAND_NAME) LIKE UPPER('%CLINTON, HILLARY%') OR UPPER(CAND_NAME) LIKE UPPER('%SANDERS, BERNARD%') " +
      "OR UPPER(CAND_NAME) LIKE UPPER('%Trump, Donald%') OR UPPER(CAND_NAME) LIKE UPPER('%CRUZ, RAFAEL%'))")
    candidates.createOrReplaceTempView("candidates")
    candidates.show()

    //Q2
    val contFromInd = itcontDF.join(candidates, candidates("CAND_PCC")===itcontDF("CMTE_ID")).select("CAND_ID", "CAND_PCC", "CAND_NAME", "TRANSACTION_AMT", "SUB_ID").where("TRANSACTION_AMT>0")
    contFromInd.groupBy(candidates("CAND_ID"), candidates("CAND_NAME")).count().show()

    //Q3
    contFromInd.groupBy(candidates("CAND_ID"), candidates("CAND_NAME")).sum("TRANSACTION_AMT").show()

    //Q4
    cclDF.join(candidates, candidates("CAND_ID")===cclDF("CAND_ID")).select(candidates("CAND_ID"), candidates("CAND_NAME"), cclDF("CMTE_ID")).show()

    //Q5
    val contFromCom = itpasDF.join(candidates, candidates("CAND_ID")===itpasDF("CAND_ID")).select(candidates("CAND_ID"), candidates("CAND_NAME"), itpasDF("CMTE_ID"), itpasDF("TRANSACTION_AMT")).where("TRANSACTION_AMT>0")
    contFromCom.groupBy(candidates("CAND_ID"), candidates("CAND_NAME")).count().show()

    //Q6
    contFromCom.groupBy(candidates("CAND_ID"), candidates("CAND_NAME")).sum("TRANSACTION_AMT").show()

  }
}
