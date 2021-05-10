package codigo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}

import scala.util.control.Exception

object Dinamicos {
  def main(args: Array[String]): Unit = {


    //Inicializamos la sesión de Spark y configuramos el tipo de logs

    val logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("")
    logger.setLevel(Level.ERROR)

    val kafka_logger =Logger.getLogger("kafka")
    kafka_logger.setLevel(Level.WARN)

    val spark = SparkSession.builder
      .appName("Dinamicos")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext


    //#############################################################

    //Schema para los datos que vamos a leer
    val schema =  StructType(Seq(StructField("TransactionID",DoubleType,true), StructField("id_01",DoubleType,true), StructField("id_02",DoubleType,true), StructField("id_05",DoubleType,true), StructField("id_06",DoubleType,true), StructField("id_11",DoubleType,true), StructField("id_13",DoubleType,true), StructField("id_17",DoubleType,true), StructField("id_19",DoubleType,true), StructField("id_20",DoubleType,true), StructField("TransactionDT",DoubleType,true), StructField("TransactionAmt",DoubleType,true), StructField("card1",DoubleType,true), StructField("card2",DoubleType,true), StructField("card3",DoubleType,true), StructField("card5",DoubleType,true), StructField("C1",DoubleType,true), StructField("C2",DoubleType,true), StructField("C3",DoubleType,true), StructField("C4",DoubleType,true), StructField("C5",DoubleType,true), StructField("C6",DoubleType,true), StructField("C7",DoubleType,true), StructField("C8",DoubleType,true), StructField("C9",DoubleType,true), StructField("C10",DoubleType,true), StructField("C11",DoubleType,true), StructField("C12",DoubleType,true), StructField("C13",DoubleType,true), StructField("C14",DoubleType,true), StructField("D1",DoubleType,true), StructField("V95",DoubleType,true), StructField("V96",DoubleType,true), StructField("V97",DoubleType,true), StructField("V98",DoubleType,true), StructField("V99",DoubleType,true), StructField("V100",DoubleType,true), StructField("V101",DoubleType,true), StructField("V102",DoubleType,true), StructField("V103",DoubleType,true), StructField("V104",DoubleType,true), StructField("V105",DoubleType,true), StructField("V106",DoubleType,true), StructField("V107",DoubleType,true), StructField("V108",DoubleType,true), StructField("V109",DoubleType,true), StructField("V110",DoubleType,true), StructField("V111",DoubleType,true), StructField("V112",DoubleType,true), StructField("V113",DoubleType,true), StructField("V114",DoubleType,true), StructField("V115",DoubleType,true), StructField("V116",DoubleType,true), StructField("V117",DoubleType,true), StructField("V118",DoubleType,true), StructField("V119",DoubleType,true), StructField("V120",DoubleType,true), StructField("V121",DoubleType,true), StructField("V122",DoubleType,true), StructField("V123",DoubleType,true), StructField("V124",DoubleType,true), StructField("V125",DoubleType,true), StructField("V126",DoubleType,true), StructField("V127",DoubleType,true), StructField("V128",DoubleType,true), StructField("V129",DoubleType,true), StructField("V130",DoubleType,true), StructField("V131",DoubleType,true), StructField("V132",DoubleType,true), StructField("V133",DoubleType,true), StructField("V134",DoubleType,true), StructField("V135",DoubleType,true), StructField("V136",DoubleType,true), StructField("V137",DoubleType,true), StructField("V167",DoubleType,true), StructField("V168",DoubleType,true), StructField("V169",DoubleType,true), StructField("V170",DoubleType,true), StructField("V171",DoubleType,true), StructField("V172",DoubleType,true), StructField("V173",DoubleType,true), StructField("V174",DoubleType,true), StructField("V175",DoubleType,true), StructField("V176",DoubleType,true), StructField("V177",DoubleType,true), StructField("V178",DoubleType,true), StructField("V179",DoubleType,true), StructField("V180",DoubleType,true), StructField("V181",DoubleType,true), StructField("V182",DoubleType,true), StructField("V183",DoubleType,true), StructField("V184",DoubleType,true), StructField("V185",DoubleType,true), StructField("V186",DoubleType,true), StructField("V187",DoubleType,true), StructField("V188",DoubleType,true), StructField("V189",DoubleType,true), StructField("V190",DoubleType,true), StructField("V191",DoubleType,true), StructField("V192",DoubleType,true), StructField("V193",DoubleType,true), StructField("V194",DoubleType,true), StructField("V195",DoubleType,true), StructField("V196",DoubleType,true), StructField("V197",DoubleType,true), StructField("V198",DoubleType,true), StructField("V199",DoubleType,true), StructField("V200",DoubleType,true), StructField("V201",DoubleType,true), StructField("V202",DoubleType,true), StructField("V203",DoubleType,true), StructField("V204",DoubleType,true), StructField("V205",DoubleType,true), StructField("V206",DoubleType,true), StructField("V207",DoubleType,true), StructField("V208",DoubleType,true), StructField("V209",DoubleType,true), StructField("V210",DoubleType,true), StructField("V211",DoubleType,true), StructField("V212",DoubleType,true), StructField("V213",DoubleType,true), StructField("V214",DoubleType,true), StructField("V215",DoubleType,true), StructField("V216",DoubleType,true), StructField("V217",DoubleType,true), StructField("V218",DoubleType,true), StructField("V219",DoubleType,true), StructField("V220",DoubleType,true), StructField("V221",DoubleType,true), StructField("V222",DoubleType,true), StructField("V223",DoubleType,true), StructField("V224",DoubleType,true), StructField("V225",DoubleType,true), StructField("V226",DoubleType,true), StructField("V227",DoubleType,true), StructField("V228",DoubleType,true), StructField("V229",DoubleType,true), StructField("V230",DoubleType,true), StructField("V231",DoubleType,true), StructField("V232",DoubleType,true), StructField("V233",DoubleType,true), StructField("V234",DoubleType,true), StructField("V235",DoubleType,true), StructField("V236",DoubleType,true), StructField("V237",DoubleType,true), StructField("V238",DoubleType,true), StructField("V239",DoubleType,true), StructField("V240",DoubleType,true), StructField("V241",DoubleType,true), StructField("V242",DoubleType,true), StructField("V243",DoubleType,true), StructField("V244",DoubleType,true), StructField("V245",DoubleType,true), StructField("V246",DoubleType,true), StructField("V247",DoubleType,true), StructField("V248",DoubleType,true), StructField("V249",DoubleType,true), StructField("V250",DoubleType,true), StructField("V251",DoubleType,true), StructField("V252",DoubleType,true), StructField("V253",DoubleType,true), StructField("V254",DoubleType,true), StructField("V255",DoubleType,true), StructField("V256",DoubleType,true), StructField("V257",DoubleType,true), StructField("V258",DoubleType,true), StructField("V259",DoubleType,true), StructField("V260",DoubleType,true), StructField("V261",DoubleType,true), StructField("V262",DoubleType,true), StructField("V263",DoubleType,true), StructField("V264",DoubleType,true), StructField("V265",DoubleType,true), StructField("V266",DoubleType,true), StructField("V267",DoubleType,true), StructField("V268",DoubleType,true), StructField("V269",DoubleType,true), StructField("V270",DoubleType,true), StructField("V271",DoubleType,true), StructField("V272",DoubleType,true), StructField("V273",DoubleType,true), StructField("V274",DoubleType,true), StructField("V275",DoubleType,true), StructField("V276",DoubleType,true), StructField("V277",DoubleType,true), StructField("V278",DoubleType,true), StructField("V279",DoubleType,true), StructField("V280",DoubleType,true), StructField("V281",DoubleType,true), StructField("V282",DoubleType,true), StructField("V283",DoubleType,true), StructField("V284",DoubleType,true), StructField("V285",DoubleType,true), StructField("V286",DoubleType,true), StructField("V287",DoubleType,true), StructField("V288",DoubleType,true), StructField("V289",DoubleType,true), StructField("V290",DoubleType,true), StructField("V291",DoubleType,true), StructField("V292",DoubleType,true), StructField("V293",DoubleType,true), StructField("V294",DoubleType,true), StructField("V295",DoubleType,true), StructField("V296",DoubleType,true), StructField("V297",DoubleType,true), StructField("V298",DoubleType,true), StructField("V299",DoubleType,true), StructField("V300",DoubleType,true), StructField("V301",DoubleType,true), StructField("V302",DoubleType,true), StructField("V303",DoubleType,true), StructField("V304",DoubleType,true), StructField("V305",DoubleType,true), StructField("V306",DoubleType,true), StructField("V307",DoubleType,true), StructField("V308",DoubleType,true), StructField("V309",DoubleType,true), StructField("V310",DoubleType,true), StructField("V311",DoubleType,true), StructField("V312",DoubleType,true), StructField("V313",DoubleType,true), StructField("V314",DoubleType,true), StructField("V315",DoubleType,true), StructField("V316",DoubleType,true), StructField("V317",DoubleType,true), StructField("V318",DoubleType,true), StructField("V319",DoubleType,true), StructField("V320",DoubleType,true), StructField("V321",DoubleType,true), StructField("n_id_12",DoubleType,false), StructField("n_id_15",DoubleType,false), StructField("n_id_16",DoubleType,false), StructField("n_id_28",DoubleType,false), StructField("n_id_29",DoubleType,false), StructField("n_id_31",DoubleType,false), StructField("n_id_35",DoubleType,false), StructField("n_id_36",DoubleType,false), StructField("n_id_37",DoubleType,false), StructField("n_id_38",DoubleType,false), StructField("n_DeviceType",DoubleType,false), StructField("n_ProductCD",DoubleType,false), StructField("n_card4",DoubleType,false), StructField("n_card6",DoubleType,false), StructField("n_P_emaildomain",DoubleType,false), StructField("n_R_emaildomain",DoubleType,false)))

    //Conexion con Kafka

    val kafka_broker_hostname = "localhost"
    val kafka_consumer_portno = "9095"
    val kafka_broker = kafka_broker_hostname + ":" + kafka_consumer_portno
    val kafka_topic_input = "test"


    val df_kafka = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",kafka_broker)
        .option("subscribe",kafka_topic_input)
        .load()

    val df_kafka_string = df_kafka.selectExpr("CAST(value AS STRING) as value")

    val df_kafka_string_parsed = df_kafka_string
      .select(from_json(col("value"),schema).alias("data"))

    val new_data = df_kafka_string_parsed.select("data.*")

    new_data.printSchema()

    val assembler = new VectorAssembler()
      .setInputCols(new_data.columns)
      .setHandleInvalid("keep")
      .setOutputCol("features")

    val data_df = assembler.transform(new_data)

    val model = GBTClassificationModel.load("/user/jmontero/TFM/Modelo")

    val resultado = model.transform(data_df)

    val res = resultado.select("TransactionID","prediction")

    res
      .writeStream
      .format("csv")
      .trigger(Trigger.ProcessingTime(5000))
      .option("checkpointLocation", "/user/jmontero/TFM/Result/Dynamic")
      .option("path","/user/jmontero/TFM/Result/Dynamic") 
      .outputMode("append")
      .start()
      .awaitTermination()


  }
}
