package codigo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.util.Objects

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}

object Estaticos {
  def main(args: Array[String]): Unit = {


    //Inicializamos la sesión de Spark y configuramos el tipo de logs

    val logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("Estaticos")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext


    //#############################################################


    //Cargamos los datos

    println("#############################################################")
    println("#                Cargamos los datos                         #")
    println("#############################################################")

    val train_identity_schema =  StructType(Seq(StructField("TransactionID",IntegerType,true), StructField("id_01",DoubleType,true), StructField("id_02",DoubleType,true), StructField("id_03",DoubleType,true), StructField("id_04",DoubleType,true), StructField("id_05",DoubleType,true), StructField("id_06",DoubleType,true), StructField("id_07",DoubleType,true), StructField("id_08",DoubleType,true), StructField("id_09",DoubleType,true), StructField("id_10",DoubleType,true), StructField("id_11",DoubleType,true), StructField("id_12",StringType,true), StructField("id_13",DoubleType,true), StructField("id_14",DoubleType,true), StructField("id_15",StringType,true), StructField("id_16",StringType,true), StructField("id_17",DoubleType,true), StructField("id_18",DoubleType,true), StructField("id_19",DoubleType,true), StructField("id_20",DoubleType,true), StructField("id_21",DoubleType,true), StructField("id_22",DoubleType,true), StructField("id_23",StringType,true), StructField("id_24",DoubleType,true), StructField("id_25",DoubleType,true), StructField("id_26",DoubleType,true), StructField("id_27",StringType,true), StructField("id_28",StringType,true), StructField("id_29",StringType,true), StructField("id_30",StringType,true), StructField("id_31",StringType,true), StructField("id_32",DoubleType,true), StructField("id_33",StringType,true), StructField("id_34",StringType,true), StructField("id_35",StringType,true), StructField("id_36",StringType,true), StructField("id_37",StringType,true), StructField("id_38",StringType,true), StructField("DeviceType",StringType,true), StructField("DeviceInfo",StringType,true)))
    val train_transaction_schema =  StructType(Seq(StructField("TransactionID",IntegerType,true), StructField("isFraud",IntegerType,true), StructField("TransactionDT",IntegerType,true), StructField("TransactionAmt",DoubleType,true), StructField("ProductCD",StringType,true), StructField("card1",IntegerType,true), StructField("card2",DoubleType,true), StructField("card3",DoubleType,true), StructField("card4",StringType,true), StructField("card5",DoubleType,true), StructField("card6",StringType,true), StructField("addr1",DoubleType,true), StructField("addr2",DoubleType,true), StructField("dist1",DoubleType,true), StructField("dist2",DoubleType,true), StructField("P_emaildomain",StringType,true), StructField("R_emaildomain",StringType,true), StructField("C1",DoubleType,true), StructField("C2",DoubleType,true), StructField("C3",DoubleType,true), StructField("C4",DoubleType,true), StructField("C5",DoubleType,true), StructField("C6",DoubleType,true), StructField("C7",DoubleType,true), StructField("C8",DoubleType,true), StructField("C9",DoubleType,true), StructField("C10",DoubleType,true), StructField("C11",DoubleType,true), StructField("C12",DoubleType,true), StructField("C13",DoubleType,true), StructField("C14",DoubleType,true), StructField("D1",DoubleType,true), StructField("D2",DoubleType,true), StructField("D3",DoubleType,true), StructField("D4",DoubleType,true), StructField("D5",DoubleType,true), StructField("D6",DoubleType,true), StructField("D7",DoubleType,true), StructField("D8",DoubleType,true), StructField("D9",DoubleType,true), StructField("D10",DoubleType,true), StructField("D11",DoubleType,true), StructField("D12",DoubleType,true), StructField("D13",DoubleType,true), StructField("D14",DoubleType,true), StructField("D15",DoubleType,true), StructField("M1",StringType,true), StructField("M2",StringType,true), StructField("M3",StringType,true), StructField("M4",StringType,true), StructField("M5",StringType,true), StructField("M6",StringType,true), StructField("M7",StringType,true), StructField("M8",StringType,true), StructField("M9",StringType,true), StructField("V1",DoubleType,true), StructField("V2",DoubleType,true), StructField("V3",DoubleType,true), StructField("V4",DoubleType,true), StructField("V5",DoubleType,true), StructField("V6",DoubleType,true), StructField("V7",DoubleType,true), StructField("V8",DoubleType,true), StructField("V9",DoubleType,true), StructField("V10",DoubleType,true), StructField("V11",DoubleType,true), StructField("V12",DoubleType,true), StructField("V13",DoubleType,true), StructField("V14",DoubleType,true), StructField("V15",DoubleType,true), StructField("V16",DoubleType,true), StructField("V17",DoubleType,true), StructField("V18",DoubleType,true), StructField("V19",DoubleType,true), StructField("V20",DoubleType,true), StructField("V21",DoubleType,true), StructField("V22",DoubleType,true), StructField("V23",DoubleType,true), StructField("V24",DoubleType,true), StructField("V25",DoubleType,true), StructField("V26",DoubleType,true), StructField("V27",DoubleType,true), StructField("V28",DoubleType,true), StructField("V29",DoubleType,true), StructField("V30",DoubleType,true), StructField("V31",DoubleType,true), StructField("V32",DoubleType,true), StructField("V33",DoubleType,true), StructField("V34",DoubleType,true), StructField("V35",DoubleType,true), StructField("V36",DoubleType,true), StructField("V37",DoubleType,true), StructField("V38",DoubleType,true), StructField("V39",DoubleType,true), StructField("V40",DoubleType,true), StructField("V41",DoubleType,true), StructField("V42",DoubleType,true), StructField("V43",DoubleType,true), StructField("V44",DoubleType,true), StructField("V45",DoubleType,true), StructField("V46",DoubleType,true), StructField("V47",DoubleType,true), StructField("V48",DoubleType,true), StructField("V49",DoubleType,true), StructField("V50",DoubleType,true), StructField("V51",DoubleType,true), StructField("V52",DoubleType,true), StructField("V53",DoubleType,true), StructField("V54",DoubleType,true), StructField("V55",DoubleType,true), StructField("V56",DoubleType,true), StructField("V57",DoubleType,true), StructField("V58",DoubleType,true), StructField("V59",DoubleType,true), StructField("V60",DoubleType,true), StructField("V61",DoubleType,true), StructField("V62",DoubleType,true), StructField("V63",DoubleType,true), StructField("V64",DoubleType,true), StructField("V65",DoubleType,true), StructField("V66",DoubleType,true), StructField("V67",DoubleType,true), StructField("V68",DoubleType,true), StructField("V69",DoubleType,true), StructField("V70",DoubleType,true), StructField("V71",DoubleType,true), StructField("V72",DoubleType,true), StructField("V73",DoubleType,true), StructField("V74",DoubleType,true), StructField("V75",DoubleType,true), StructField("V76",DoubleType,true), StructField("V77",DoubleType,true), StructField("V78",DoubleType,true), StructField("V79",DoubleType,true), StructField("V80",DoubleType,true), StructField("V81",DoubleType,true), StructField("V82",DoubleType,true), StructField("V83",DoubleType,true), StructField("V84",DoubleType,true), StructField("V85",DoubleType,true), StructField("V86",DoubleType,true), StructField("V87",DoubleType,true), StructField("V88",DoubleType,true), StructField("V89",DoubleType,true), StructField("V90",DoubleType,true), StructField("V91",DoubleType,true), StructField("V92",DoubleType,true), StructField("V93",DoubleType,true), StructField("V94",DoubleType,true), StructField("V95",DoubleType,true), StructField("V96",DoubleType,true), StructField("V97",DoubleType,true), StructField("V98",DoubleType,true), StructField("V99",DoubleType,true), StructField("V100",DoubleType,true), StructField("V101",DoubleType,true), StructField("V102",DoubleType,true), StructField("V103",DoubleType,true), StructField("V104",DoubleType,true), StructField("V105",DoubleType,true), StructField("V106",DoubleType,true), StructField("V107",DoubleType,true), StructField("V108",DoubleType,true), StructField("V109",DoubleType,true), StructField("V110",DoubleType,true), StructField("V111",DoubleType,true), StructField("V112",DoubleType,true), StructField("V113",DoubleType,true), StructField("V114",DoubleType,true), StructField("V115",DoubleType,true), StructField("V116",DoubleType,true), StructField("V117",DoubleType,true), StructField("V118",DoubleType,true), StructField("V119",DoubleType,true), StructField("V120",DoubleType,true), StructField("V121",DoubleType,true), StructField("V122",DoubleType,true), StructField("V123",DoubleType,true), StructField("V124",DoubleType,true), StructField("V125",DoubleType,true), StructField("V126",DoubleType,true), StructField("V127",DoubleType,true), StructField("V128",DoubleType,true), StructField("V129",DoubleType,true), StructField("V130",DoubleType,true), StructField("V131",DoubleType,true), StructField("V132",DoubleType,true), StructField("V133",DoubleType,true), StructField("V134",DoubleType,true), StructField("V135",DoubleType,true), StructField("V136",DoubleType,true), StructField("V137",DoubleType,true), StructField("V138",DoubleType,true), StructField("V139",DoubleType,true), StructField("V140",DoubleType,true), StructField("V141",DoubleType,true), StructField("V142",DoubleType,true), StructField("V143",DoubleType,true), StructField("V144",DoubleType,true), StructField("V145",DoubleType,true), StructField("V146",DoubleType,true), StructField("V147",DoubleType,true), StructField("V148",DoubleType,true), StructField("V149",DoubleType,true), StructField("V150",DoubleType,true), StructField("V151",DoubleType,true), StructField("V152",DoubleType,true), StructField("V153",DoubleType,true), StructField("V154",DoubleType,true), StructField("V155",DoubleType,true), StructField("V156",DoubleType,true), StructField("V157",DoubleType,true), StructField("V158",DoubleType,true), StructField("V159",DoubleType,true), StructField("V160",DoubleType,true), StructField("V161",DoubleType,true), StructField("V162",DoubleType,true), StructField("V163",DoubleType,true), StructField("V164",DoubleType,true), StructField("V165",DoubleType,true), StructField("V166",DoubleType,true), StructField("V167",DoubleType,true), StructField("V168",DoubleType,true), StructField("V169",DoubleType,true), StructField("V170",DoubleType,true), StructField("V171",DoubleType,true), StructField("V172",DoubleType,true), StructField("V173",DoubleType,true), StructField("V174",DoubleType,true), StructField("V175",DoubleType,true), StructField("V176",DoubleType,true), StructField("V177",DoubleType,true), StructField("V178",DoubleType,true), StructField("V179",DoubleType,true), StructField("V180",DoubleType,true), StructField("V181",DoubleType,true), StructField("V182",DoubleType,true), StructField("V183",DoubleType,true), StructField("V184",DoubleType,true), StructField("V185",DoubleType,true), StructField("V186",DoubleType,true), StructField("V187",DoubleType,true), StructField("V188",DoubleType,true), StructField("V189",DoubleType,true), StructField("V190",DoubleType,true), StructField("V191",DoubleType,true), StructField("V192",DoubleType,true), StructField("V193",DoubleType,true), StructField("V194",DoubleType,true), StructField("V195",DoubleType,true), StructField("V196",DoubleType,true), StructField("V197",DoubleType,true), StructField("V198",DoubleType,true), StructField("V199",DoubleType,true), StructField("V200",DoubleType,true), StructField("V201",DoubleType,true), StructField("V202",DoubleType,true), StructField("V203",DoubleType,true), StructField("V204",DoubleType,true), StructField("V205",DoubleType,true), StructField("V206",DoubleType,true), StructField("V207",DoubleType,true), StructField("V208",DoubleType,true), StructField("V209",DoubleType,true), StructField("V210",DoubleType,true), StructField("V211",DoubleType,true), StructField("V212",DoubleType,true), StructField("V213",DoubleType,true), StructField("V214",DoubleType,true), StructField("V215",DoubleType,true), StructField("V216",DoubleType,true), StructField("V217",DoubleType,true), StructField("V218",DoubleType,true), StructField("V219",DoubleType,true), StructField("V220",DoubleType,true), StructField("V221",DoubleType,true), StructField("V222",DoubleType,true), StructField("V223",DoubleType,true), StructField("V224",DoubleType,true), StructField("V225",DoubleType,true), StructField("V226",DoubleType,true), StructField("V227",DoubleType,true), StructField("V228",DoubleType,true), StructField("V229",DoubleType,true), StructField("V230",DoubleType,true), StructField("V231",DoubleType,true), StructField("V232",DoubleType,true), StructField("V233",DoubleType,true), StructField("V234",DoubleType,true), StructField("V235",DoubleType,true), StructField("V236",DoubleType,true), StructField("V237",DoubleType,true), StructField("V238",DoubleType,true), StructField("V239",DoubleType,true), StructField("V240",DoubleType,true), StructField("V241",DoubleType,true), StructField("V242",DoubleType,true), StructField("V243",DoubleType,true), StructField("V244",DoubleType,true), StructField("V245",DoubleType,true), StructField("V246",DoubleType,true), StructField("V247",DoubleType,true), StructField("V248",DoubleType,true), StructField("V249",DoubleType,true), StructField("V250",DoubleType,true), StructField("V251",DoubleType,true), StructField("V252",DoubleType,true), StructField("V253",DoubleType,true), StructField("V254",DoubleType,true), StructField("V255",DoubleType,true), StructField("V256",DoubleType,true), StructField("V257",DoubleType,true), StructField("V258",DoubleType,true), StructField("V259",DoubleType,true), StructField("V260",DoubleType,true), StructField("V261",DoubleType,true), StructField("V262",DoubleType,true), StructField("V263",DoubleType,true), StructField("V264",DoubleType,true), StructField("V265",DoubleType,true), StructField("V266",DoubleType,true), StructField("V267",DoubleType,true), StructField("V268",DoubleType,true), StructField("V269",DoubleType,true), StructField("V270",DoubleType,true), StructField("V271",DoubleType,true), StructField("V272",DoubleType,true), StructField("V273",DoubleType,true), StructField("V274",DoubleType,true), StructField("V275",DoubleType,true), StructField("V276",DoubleType,true), StructField("V277",DoubleType,true), StructField("V278",DoubleType,true), StructField("V279",DoubleType,true), StructField("V280",DoubleType,true), StructField("V281",DoubleType,true), StructField("V282",DoubleType,true), StructField("V283",DoubleType,true), StructField("V284",DoubleType,true), StructField("V285",DoubleType,true), StructField("V286",DoubleType,true), StructField("V287",DoubleType,true), StructField("V288",DoubleType,true), StructField("V289",DoubleType,true), StructField("V290",DoubleType,true), StructField("V291",DoubleType,true), StructField("V292",DoubleType,true), StructField("V293",DoubleType,true), StructField("V294",DoubleType,true), StructField("V295",DoubleType,true), StructField("V296",DoubleType,true), StructField("V297",DoubleType,true), StructField("V298",DoubleType,true), StructField("V299",DoubleType,true), StructField("V300",DoubleType,true), StructField("V301",DoubleType,true), StructField("V302",DoubleType,true), StructField("V303",DoubleType,true), StructField("V304",DoubleType,true), StructField("V305",DoubleType,true), StructField("V306",DoubleType,true), StructField("V307",DoubleType,true), StructField("V308",DoubleType,true), StructField("V309",DoubleType,true), StructField("V310",DoubleType,true), StructField("V311",DoubleType,true), StructField("V312",DoubleType,true), StructField("V313",DoubleType,true), StructField("V314",DoubleType,true), StructField("V315",DoubleType,true), StructField("V316",DoubleType,true), StructField("V317",DoubleType,true), StructField("V318",DoubleType,true), StructField("V319",DoubleType,true), StructField("V320",DoubleType,true), StructField("V321",DoubleType,true), StructField("V322",DoubleType,true), StructField("V323",DoubleType,true), StructField("V324",DoubleType,true), StructField("V325",DoubleType,true), StructField("V326",DoubleType,true), StructField("V327",DoubleType,true), StructField("V328",DoubleType,true), StructField("V329",DoubleType,true), StructField("V330",DoubleType,true), StructField("V331",DoubleType,true), StructField("V332",DoubleType,true), StructField("V333",DoubleType,true), StructField("V334",DoubleType,true), StructField("V335",DoubleType,true), StructField("V336",DoubleType,true), StructField("V337",DoubleType,true), StructField("V338",DoubleType,true), StructField("V339",DoubleType,true)))
    val test_identity_schema =  StructType(Seq(StructField("TransactionID",IntegerType,true), StructField("id_01",DoubleType,true), StructField("id_02",DoubleType,true), StructField("id_03",DoubleType,true), StructField("id_04",DoubleType,true), StructField("id_05",DoubleType,true), StructField("id_06",DoubleType,true), StructField("id_07",DoubleType,true), StructField("id_08",DoubleType,true), StructField("id_09",DoubleType,true), StructField("id_10",DoubleType,true), StructField("id_11",DoubleType,true), StructField("id_12",StringType,true), StructField("id_13",DoubleType,true), StructField("id_14",DoubleType,true), StructField("id_15",StringType,true), StructField("id_16",StringType,true), StructField("id_17",DoubleType,true), StructField("id_18",DoubleType,true), StructField("id_19",DoubleType,true), StructField("id_20",DoubleType,true), StructField("id_21",DoubleType,true), StructField("id_22",DoubleType,true), StructField("id_23",StringType,true), StructField("id_24",DoubleType,true), StructField("id_25",DoubleType,true), StructField("id_26",DoubleType,true), StructField("id_27",StringType,true), StructField("id_28",StringType,true), StructField("id_29",StringType,true), StructField("id_30",StringType,true), StructField("id_31",StringType,true), StructField("id_32",DoubleType,true), StructField("id_33",StringType,true), StructField("id_34",StringType,true), StructField("id_35",StringType,true), StructField("id_36",StringType,true), StructField("id_37",StringType,true), StructField("id_38",StringType,true), StructField("DeviceType",StringType,true), StructField("DeviceInfo",StringType,true)))
    val test_transaction_schema =  StructType(Seq(StructField("TransactionID",IntegerType,true), StructField("isFraud",IntegerType,true), StructField("TransactionDT",IntegerType,true), StructField("TransactionAmt",DoubleType,true), StructField("ProductCD",StringType,true), StructField("card1",IntegerType,true), StructField("card2",DoubleType,true), StructField("card3",DoubleType,true), StructField("card4",StringType,true), StructField("card5",DoubleType,true), StructField("card6",StringType,true), StructField("addr1",DoubleType,true), StructField("addr2",DoubleType,true), StructField("dist1",DoubleType,true), StructField("dist2",DoubleType,true), StructField("P_emaildomain",StringType,true), StructField("R_emaildomain",StringType,true), StructField("C1",DoubleType,true), StructField("C2",DoubleType,true), StructField("C3",DoubleType,true), StructField("C4",DoubleType,true), StructField("C5",DoubleType,true), StructField("C6",DoubleType,true), StructField("C7",DoubleType,true), StructField("C8",DoubleType,true), StructField("C9",DoubleType,true), StructField("C10",DoubleType,true), StructField("C11",DoubleType,true), StructField("C12",DoubleType,true), StructField("C13",DoubleType,true), StructField("C14",DoubleType,true), StructField("D1",DoubleType,true), StructField("D2",DoubleType,true), StructField("D3",DoubleType,true), StructField("D4",DoubleType,true), StructField("D5",DoubleType,true), StructField("D6",DoubleType,true), StructField("D7",DoubleType,true), StructField("D8",DoubleType,true), StructField("D9",DoubleType,true), StructField("D10",DoubleType,true), StructField("D11",DoubleType,true), StructField("D12",DoubleType,true), StructField("D13",DoubleType,true), StructField("D14",DoubleType,true), StructField("D15",DoubleType,true), StructField("M1",StringType,true), StructField("M2",StringType,true), StructField("M3",StringType,true), StructField("M4",StringType,true), StructField("M5",StringType,true), StructField("M6",StringType,true), StructField("M7",StringType,true), StructField("M8",StringType,true), StructField("M9",StringType,true), StructField("V1",DoubleType,true), StructField("V2",DoubleType,true), StructField("V3",DoubleType,true), StructField("V4",DoubleType,true), StructField("V5",DoubleType,true), StructField("V6",DoubleType,true), StructField("V7",DoubleType,true), StructField("V8",DoubleType,true), StructField("V9",DoubleType,true), StructField("V10",DoubleType,true), StructField("V11",DoubleType,true), StructField("V12",DoubleType,true), StructField("V13",DoubleType,true), StructField("V14",DoubleType,true), StructField("V15",DoubleType,true), StructField("V16",DoubleType,true), StructField("V17",DoubleType,true), StructField("V18",DoubleType,true), StructField("V19",DoubleType,true), StructField("V20",DoubleType,true), StructField("V21",DoubleType,true), StructField("V22",DoubleType,true), StructField("V23",DoubleType,true), StructField("V24",DoubleType,true), StructField("V25",DoubleType,true), StructField("V26",DoubleType,true), StructField("V27",DoubleType,true), StructField("V28",DoubleType,true), StructField("V29",DoubleType,true), StructField("V30",DoubleType,true), StructField("V31",DoubleType,true), StructField("V32",DoubleType,true), StructField("V33",DoubleType,true), StructField("V34",DoubleType,true), StructField("V35",DoubleType,true), StructField("V36",DoubleType,true), StructField("V37",DoubleType,true), StructField("V38",DoubleType,true), StructField("V39",DoubleType,true), StructField("V40",DoubleType,true), StructField("V41",DoubleType,true), StructField("V42",DoubleType,true), StructField("V43",DoubleType,true), StructField("V44",DoubleType,true), StructField("V45",DoubleType,true), StructField("V46",DoubleType,true), StructField("V47",DoubleType,true), StructField("V48",DoubleType,true), StructField("V49",DoubleType,true), StructField("V50",DoubleType,true), StructField("V51",DoubleType,true), StructField("V52",DoubleType,true), StructField("V53",DoubleType,true), StructField("V54",DoubleType,true), StructField("V55",DoubleType,true), StructField("V56",DoubleType,true), StructField("V57",DoubleType,true), StructField("V58",DoubleType,true), StructField("V59",DoubleType,true), StructField("V60",DoubleType,true), StructField("V61",DoubleType,true), StructField("V62",DoubleType,true), StructField("V63",DoubleType,true), StructField("V64",DoubleType,true), StructField("V65",DoubleType,true), StructField("V66",DoubleType,true), StructField("V67",DoubleType,true), StructField("V68",DoubleType,true), StructField("V69",DoubleType,true), StructField("V70",DoubleType,true), StructField("V71",DoubleType,true), StructField("V72",DoubleType,true), StructField("V73",DoubleType,true), StructField("V74",DoubleType,true), StructField("V75",DoubleType,true), StructField("V76",DoubleType,true), StructField("V77",DoubleType,true), StructField("V78",DoubleType,true), StructField("V79",DoubleType,true), StructField("V80",DoubleType,true), StructField("V81",DoubleType,true), StructField("V82",DoubleType,true), StructField("V83",DoubleType,true), StructField("V84",DoubleType,true), StructField("V85",DoubleType,true), StructField("V86",DoubleType,true), StructField("V87",DoubleType,true), StructField("V88",DoubleType,true), StructField("V89",DoubleType,true), StructField("V90",DoubleType,true), StructField("V91",DoubleType,true), StructField("V92",DoubleType,true), StructField("V93",DoubleType,true), StructField("V94",DoubleType,true), StructField("V95",DoubleType,true), StructField("V96",DoubleType,true), StructField("V97",DoubleType,true), StructField("V98",DoubleType,true), StructField("V99",DoubleType,true), StructField("V100",DoubleType,true), StructField("V101",DoubleType,true), StructField("V102",DoubleType,true), StructField("V103",DoubleType,true), StructField("V104",DoubleType,true), StructField("V105",DoubleType,true), StructField("V106",DoubleType,true), StructField("V107",DoubleType,true), StructField("V108",DoubleType,true), StructField("V109",DoubleType,true), StructField("V110",DoubleType,true), StructField("V111",DoubleType,true), StructField("V112",DoubleType,true), StructField("V113",DoubleType,true), StructField("V114",DoubleType,true), StructField("V115",DoubleType,true), StructField("V116",DoubleType,true), StructField("V117",DoubleType,true), StructField("V118",DoubleType,true), StructField("V119",DoubleType,true), StructField("V120",DoubleType,true), StructField("V121",DoubleType,true), StructField("V122",DoubleType,true), StructField("V123",DoubleType,true), StructField("V124",DoubleType,true), StructField("V125",DoubleType,true), StructField("V126",DoubleType,true), StructField("V127",DoubleType,true), StructField("V128",DoubleType,true), StructField("V129",DoubleType,true), StructField("V130",DoubleType,true), StructField("V131",DoubleType,true), StructField("V132",DoubleType,true), StructField("V133",DoubleType,true), StructField("V134",DoubleType,true), StructField("V135",DoubleType,true), StructField("V136",DoubleType,true), StructField("V137",DoubleType,true), StructField("V138",DoubleType,true), StructField("V139",DoubleType,true), StructField("V140",DoubleType,true), StructField("V141",DoubleType,true), StructField("V142",DoubleType,true), StructField("V143",DoubleType,true), StructField("V144",DoubleType,true), StructField("V145",DoubleType,true), StructField("V146",DoubleType,true), StructField("V147",DoubleType,true), StructField("V148",DoubleType,true), StructField("V149",DoubleType,true), StructField("V150",DoubleType,true), StructField("V151",DoubleType,true), StructField("V152",DoubleType,true), StructField("V153",DoubleType,true), StructField("V154",DoubleType,true), StructField("V155",DoubleType,true), StructField("V156",DoubleType,true), StructField("V157",DoubleType,true), StructField("V158",DoubleType,true), StructField("V159",DoubleType,true), StructField("V160",DoubleType,true), StructField("V161",DoubleType,true), StructField("V162",DoubleType,true), StructField("V163",DoubleType,true), StructField("V164",DoubleType,true), StructField("V165",DoubleType,true), StructField("V166",DoubleType,true), StructField("V167",DoubleType,true), StructField("V168",DoubleType,true), StructField("V169",DoubleType,true), StructField("V170",DoubleType,true), StructField("V171",DoubleType,true), StructField("V172",DoubleType,true), StructField("V173",DoubleType,true), StructField("V174",DoubleType,true), StructField("V175",DoubleType,true), StructField("V176",DoubleType,true), StructField("V177",DoubleType,true), StructField("V178",DoubleType,true), StructField("V179",DoubleType,true), StructField("V180",DoubleType,true), StructField("V181",DoubleType,true), StructField("V182",DoubleType,true), StructField("V183",DoubleType,true), StructField("V184",DoubleType,true), StructField("V185",DoubleType,true), StructField("V186",DoubleType,true), StructField("V187",DoubleType,true), StructField("V188",DoubleType,true), StructField("V189",DoubleType,true), StructField("V190",DoubleType,true), StructField("V191",DoubleType,true), StructField("V192",DoubleType,true), StructField("V193",DoubleType,true), StructField("V194",DoubleType,true), StructField("V195",DoubleType,true), StructField("V196",DoubleType,true), StructField("V197",DoubleType,true), StructField("V198",DoubleType,true), StructField("V199",DoubleType,true), StructField("V200",DoubleType,true), StructField("V201",DoubleType,true), StructField("V202",DoubleType,true), StructField("V203",DoubleType,true), StructField("V204",DoubleType,true), StructField("V205",DoubleType,true), StructField("V206",DoubleType,true), StructField("V207",DoubleType,true), StructField("V208",DoubleType,true), StructField("V209",DoubleType,true), StructField("V210",DoubleType,true), StructField("V211",DoubleType,true), StructField("V212",DoubleType,true), StructField("V213",DoubleType,true), StructField("V214",DoubleType,true), StructField("V215",DoubleType,true), StructField("V216",DoubleType,true), StructField("V217",DoubleType,true), StructField("V218",DoubleType,true), StructField("V219",DoubleType,true), StructField("V220",DoubleType,true), StructField("V221",DoubleType,true), StructField("V222",DoubleType,true), StructField("V223",DoubleType,true), StructField("V224",DoubleType,true), StructField("V225",DoubleType,true), StructField("V226",DoubleType,true), StructField("V227",DoubleType,true), StructField("V228",DoubleType,true), StructField("V229",DoubleType,true), StructField("V230",DoubleType,true), StructField("V231",DoubleType,true), StructField("V232",DoubleType,true), StructField("V233",DoubleType,true), StructField("V234",DoubleType,true), StructField("V235",DoubleType,true), StructField("V236",DoubleType,true), StructField("V237",DoubleType,true), StructField("V238",DoubleType,true), StructField("V239",DoubleType,true), StructField("V240",DoubleType,true), StructField("V241",DoubleType,true), StructField("V242",DoubleType,true), StructField("V243",DoubleType,true), StructField("V244",DoubleType,true), StructField("V245",DoubleType,true), StructField("V246",DoubleType,true), StructField("V247",DoubleType,true), StructField("V248",DoubleType,true), StructField("V249",DoubleType,true), StructField("V250",DoubleType,true), StructField("V251",DoubleType,true), StructField("V252",DoubleType,true), StructField("V253",DoubleType,true), StructField("V254",DoubleType,true), StructField("V255",DoubleType,true), StructField("V256",DoubleType,true), StructField("V257",DoubleType,true), StructField("V258",DoubleType,true), StructField("V259",DoubleType,true), StructField("V260",DoubleType,true), StructField("V261",DoubleType,true), StructField("V262",DoubleType,true), StructField("V263",DoubleType,true), StructField("V264",DoubleType,true), StructField("V265",DoubleType,true), StructField("V266",DoubleType,true), StructField("V267",DoubleType,true), StructField("V268",DoubleType,true), StructField("V269",DoubleType,true), StructField("V270",DoubleType,true), StructField("V271",DoubleType,true), StructField("V272",DoubleType,true), StructField("V273",DoubleType,true), StructField("V274",DoubleType,true), StructField("V275",DoubleType,true), StructField("V276",DoubleType,true), StructField("V277",DoubleType,true), StructField("V278",DoubleType,true), StructField("V279",DoubleType,true), StructField("V280",DoubleType,true), StructField("V281",DoubleType,true), StructField("V282",DoubleType,true), StructField("V283",DoubleType,true), StructField("V284",DoubleType,true), StructField("V285",DoubleType,true), StructField("V286",DoubleType,true), StructField("V287",DoubleType,true), StructField("V288",DoubleType,true), StructField("V289",DoubleType,true), StructField("V290",DoubleType,true), StructField("V291",DoubleType,true), StructField("V292",DoubleType,true), StructField("V293",DoubleType,true), StructField("V294",DoubleType,true), StructField("V295",DoubleType,true), StructField("V296",DoubleType,true), StructField("V297",DoubleType,true), StructField("V298",DoubleType,true), StructField("V299",DoubleType,true), StructField("V300",DoubleType,true), StructField("V301",DoubleType,true), StructField("V302",DoubleType,true), StructField("V303",DoubleType,true), StructField("V304",DoubleType,true), StructField("V305",DoubleType,true), StructField("V306",DoubleType,true), StructField("V307",DoubleType,true), StructField("V308",DoubleType,true), StructField("V309",DoubleType,true), StructField("V310",DoubleType,true), StructField("V311",DoubleType,true), StructField("V312",DoubleType,true), StructField("V313",DoubleType,true), StructField("V314",DoubleType,true), StructField("V315",DoubleType,true), StructField("V316",DoubleType,true), StructField("V317",DoubleType,true), StructField("V318",DoubleType,true), StructField("V319",DoubleType,true), StructField("V320",DoubleType,true), StructField("V321",DoubleType,true), StructField("V322",DoubleType,true), StructField("V323",DoubleType,true), StructField("V324",DoubleType,true), StructField("V325",DoubleType,true), StructField("V326",DoubleType,true), StructField("V327",DoubleType,true), StructField("V328",DoubleType,true), StructField("V329",DoubleType,true), StructField("V330",DoubleType,true), StructField("V331",DoubleType,true), StructField("V332",DoubleType,true), StructField("V333",DoubleType,true), StructField("V334",DoubleType,true), StructField("V335",DoubleType,true), StructField("V336",DoubleType,true), StructField("V337",DoubleType,true), StructField("V338",DoubleType,true), StructField("V339",DoubleType,true)))


    val train_identity = spark.read
      .option("header",true)
      .schema(train_identity_schema)
      .csv("/user/jmontero/TFM/Raw/Static/Train/Identity")

    val train_transaction = spark.read
      .option("header",true)
      .schema(train_transaction_schema)
      .csv("/user/jmontero/TFM/Raw/Static/Train/Transaction")

    val test_identity = spark.read
      .option("header",true)
      .schema(test_identity_schema)
      .csv("/user/jmontero/TFM/Raw/Static/Test/Identity")

    val test_transaction = spark.read
      .option("header",true)
      .schema(test_transaction_schema)
      .csv("/user/jmontero/TFM/Raw/Static/Test/Transaction")


    //Join a los  datasets

    println("\n------------------------------------------------------------\n")

    println("#############################################################")
    println("#                Join a los datasets                        #")
    println("#############################################################")


    val train = train_identity.join(train_transaction, "TransactionID")
    val test = test_identity.join(test_transaction, "TransactionID")

    //Mostramos el schema del dataset Train

    println("\n------------------------------------------------------------\n")

    println("#############################################################")
    println("#                    Train Schema                           #")
    println("#############################################################")

    print(train.schema)


    //Cambiamos el nombre a las columnas de test que no concuerdan con el  dataset de train

    println("\n------------------------------------------------------------\n")

    println("#############################################################")
    println("#            Modificamos columnas de test                   #")
    println("#############################################################")


    //Hacemos un mapeo de valores, cambiando "id-" por "id_"
    val string_map = Map("id-" -> "id_")
    string_map.foldLeft(test)((acc,ca) => acc.withColumnRenamed(ca._1,ca._2))

    test.printSchema()

    //Eliminamos las columnas con más de un  60% de valores nulos que encontramos haciendo EDA

    println("\n------------------------------------------------------------\n")

    println("#############################################################")
    println("#           Eliminamos columnas con >60% NA's               #")
    println("#############################################################")


    val col_dropped = List("TransactionID.","id_03","id_04","id_07","id_08","id_09","id_10","id_14","id_18","id_21","id_22","id_23",
      "id_24","id_25","id_26","id_27","id_30","id_32","id_33","id_34","addr1","addr2","dist1","dist2","D2","D3","D4",
      "D5","D6","D7","D8","D9","D10","D11","D12","D13","D14","D15","M1","M2","M3","M4","M5","M6","M7","M8","M9","V1",
      "V2","V3","V4","V5","V6","V7","V8","V9","V10","V11","V12","V13","V14","V15","V16","V17","V18","V19","V20","V21",
      "V22","V23","V24","V25","V26","V27","V28","V29","V30","V31","V32","V33","V34","V35","V36","V37","V38","V39","V40",
      "V41","V42","V43","V44","V45","V46","V47","V48","V49","V50","V51","V52","V53","V54","V55","V56","V57","V58","V59",
      "V60","V61","V62","V63","V64","V65","V66","V67","V68","V69","V70","V71","V72","V73","V74","V75","V76","V77","V78",
      "V79","V80","V81","V82","V83","V84","V85","V86","V87","V88","V89","V90","V91","V92","V93","V94","V138","V139",
      "V140","V141","V142","V143","V144","V145","V146","V147","V148","V149","V150","V151","V152","V153","V154","V155",
      "V156","V157","V158","V159","V160","V161","V162","V163","V164","V165","V166","V322","V323","V324","V325","V326",
      "V327","V328","V329","V330","V331","V332","V333","V334","V335","V336","V337","V338","V339","DeviceInfo" )

    val train_filtered = train.drop(col_dropped:_*)
    val test_filtered = test.drop(col_dropped:_*)


    //Transformamos las columnas categoricas usando el StringIndexer de Spark

    println("\n------------------------------------------------------------\n")

    println("#############################################################")
    println("#                Usamos StringIndexer                       #")
    println("#############################################################")


    //Lista de columnas a transformar
    val cat_col = List("isFraud", "id_12", "id_15", "id_16", "id_28", "id_29", "id_31", "id_35", "id_36",
    "id_37", "id_38", "DeviceType", "ProductCD", "card4",
    "card6", "P_emaildomain", "R_emaildomain")

    //Creamos el StringIndexer
    val si = new StringIndexer()

    //Creamos las variables temporales que irán almacenando los resultados de cada transformación
    var tmp = train_filtered.select(col("*"))
    var tmp2 = test_filtered.select(col("*"))

    //Map
    cat_col.foreach { s =>
      tmp = si.setInputCol(s)
        .setOutputCol("n_"+s)
        .setHandleInvalid("keep")
        .fit(tmp).transform(tmp)

      tmp2 = si.setInputCol(s)
        .setOutputCol("n_"+s)
        .setHandleInvalid("keep")
        .fit(tmp2).transform(tmp2)
    }

    //Guardamos el resultado
    val train_d = tmp.select(col("*"))
    val test_d = tmp2.select(col("*"))

    //Borramos las columnas sin procesar

    val train_df = train_d.drop(cat_col:_*)
    val test_df = test_d.drop(cat_col:_*)


    //Comprimimos las variables de análisis (menos el  target) en un vector tanto en train como en test

    println("\n------------------------------------------------------------\n")

    println("#############################################################")
    println("#                Usamos VectorAssembler                     #")
    println("#############################################################")


    val variables = train_df.drop("n_isFraud")

    val assembler = new VectorAssembler()
        .setInputCols(variables.columns)
        .setHandleInvalid("keep")
        .setOutputCol("features")

    val assembler_test = new VectorAssembler()
      .setInputCols(test_df.columns)
      .setHandleInvalid("keep")
      .setOutputCol("features")

    val featureDF = assembler.transform(train_df)
    val testData = assembler_test.transform(test_df)

    featureDF.printSchema()
    featureDF.show(10)

    //Guardamos dataset train procesado, eliminando las variables features y n_isFraud, para que el script
    //de Python lo lea y genere datos aleatorios en base a ello

    println("\n------------------------------------------------------------\n")

    println("#############################################################")
    println("#           Guardamos dataset train procesado               #")
    println("#############################################################")


    val train_csv = featureDF.select("*")

    train_csv.drop("features","n_isFraud")
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header","true")
      .csv("/user/jmontero/TFM/Raw/Dynamic")


    train_csv.drop("features","n_isFraud")
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header","true")
      .csv("/home/jmontero/TFM/Data/Dynamic")

    println("\n------------------------------------------------------------\n")

    println("#############################################################")
    println("#                      Modelado                             #")
    println("#############################################################")

    //Dividimos train data en train y validation

    val Array(training_data,valid_data) = featureDF.randomSplit(Array(0.8,0.2))

    //Instanciamos el modelo

    val gbt = new GBTClassifier()
      .setLabelCol("n_isFraud")
      .setFeaturesCol("features")
      .setLossType("logistic")
      .setMaxBins(255)
      .setMaxIter(24)
      .setMaxDepth(8)
      .setStepSize(0.05) //Learning  rate
      .setSubsamplingRate(0.7)

    val model = gbt.fit(training_data)
    println(model.toDebugString)

    //Lo aplicamos a validation data
    val validDF = model.transform(valid_data)

    //Evaluamos la validez del modelo
    val eval = new BinaryClassificationEvaluator()
      .setLabelCol("n_isFraud")
      .setMetricName("areaUnderROC")

    val accuracy = eval.evaluate(validDF)
    println(accuracy)

    //Lo aplicamos a test data
    val predictionDF = model.transform(testData)
    predictionDF.show(100)

    //Mostramos cuales son las variables con más peso
    val meta: org.apache.spark.sql.types.Metadata = predictionDF
        .schema(predictionDF.schema.fieldIndex("features"))
        .metadata

    meta.getMetadata("ml_attr").getMetadata("attrs")

    //Guardamos el resultado
    predictionDF.select("TransactionID", "prediction")
                .coalesce(1)
                .write
                .mode("overwrite").csv("/user/jmontero/TFM/Result/Static")


    model.write.overwrite()
      .save("/user/jmontero/TFM/Modelo")



}
}
