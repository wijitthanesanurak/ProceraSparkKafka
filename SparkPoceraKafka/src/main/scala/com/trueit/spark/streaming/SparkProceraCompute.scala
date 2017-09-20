package com.trueit.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext 
import org.apache.spark.sql.types.{IntegerType, DoubleType, StringType, StructField, StructType, TimestampType, FloatType}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import com.synergistic.it.util._
import java.util.concurrent.TimeUnit
import java.util.Date
import java.text.SimpleDateFormat 

case class Procera (
  probe_id					        : Long,		//1    int
  destinationipv4address	  : String,	//2    string
  destinationtransportport	: Long,		//3    int
  egressinterface			      : Long,		//4    int 
  flowendseconds			      : Long,		//5    timestamp
  flowstartseconds			    : Long,		//6    timestamp
  ingressinterface			    : Long,		//7    int
  octettotalcount			      : Long,		//8    bigint      
  packettotalcount			    : Long,		//9    bigint   
  proceraapn				        : String,	//10   string
  proceracontentcategorie	  : String,	//11   string
  proceradeviceid			      : Long,		//12   bigint
  proceraexternalrtt		    : Long,		//13   int
  proceraggsn				        : String,	//14   string
  procerahttpcontenttype	  : String,	//15   string
  procerahttpfilelength		  : Long,		//16   int
  procerahttplanguage		    : String,	//17   string
  procerahttplocation		    : String,	//18   string
  procerahttppreferer		    : String,	//19   string
  procerahttprequestmethod	: String,	//20   string
  proceraresponsestatus		  : Long,		//21   int 
  procerahttpurl			      : String,	//22   string
  procerahttpuseragent		  : String,	//23   string
  proceraimsi				        : Long,		//24   bingint
  proceraincomingoctets		  : Long,		//25   bigint
  proceraincomingpackets	  : Long,		//26   bigint
  procerainternalrtt		    : Long,		//27   int
  proceramsisdn				      : Long,		//28   bigint
  proceraoutgoingoctets		  : Long,		//29   bigint
  proceraoutgoingpackets	  : Long,		//30   bigint
  proceraqoeincomingexternal: Float,	//31   float
  proceraqoeincominginternal: Float,	//32   float
  proceraqoeoutgoingexternal: Float,	//33   float
  proceraqoeoutgoinginternal: Float,	//34   float
  procerarat				        : String,	//35   string
  proceraserverhostname		  : String,	//36   string
  proceraservice			      : String,	//37   string
  procerasgsn				        : String,	//38   string
  procerauserlocationinformation:String,//39   string
  protocolidentifier		    : Long,		//40   int
  sourceipv4address			    : String,	//41   string
  sourcetransportport		    : Long,		//42   int
  //// extended field
  bu						            : String,	//43   string
  sitename					        : String,	//44   string
  filename					        : String,   //45   string
  ld_date					          : String	//46   string
)

object SparkProceraCompute {
	val date_add = udf(() => {
    	val sdf = new SimpleDateFormat("yyyy-MM-dd")
    	val result = new Date()
  		sdf.format(result)
	})

	val encrypt: String => String = new DESedeEncryption().encrypt(_)
	val encryptUDF = udf(encrypt)
	
	val decrypt: String => String = new DESedeEncryption().decrypt(_)
	val decryptUDF = udf(decrypt)


  def doAnonymize(sqlContext:SQLContext, rdd: RDD[Array[String]], 
                  is_anonymize:String) = {
    try {
      sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
      import  sqlContext.implicits._
			val pcr = rdd.map(m=>Procera(
				m(0).toLong, 
				m(1), 
				m(2).toLong, m(3).toLong, m(4).toLong, m(5).toLong, m(6).toLong, 
				m(7).toLong, m(8).toLong, 
				m(9), m(1), 
				m(11).toLong,
        m(12).toLong,	// =>Int, 
				m(13), m(14), 
				m(15).toLong, 
				m(16), m(17), m(18), m(19),
				m(20).toLong, 
				m(21), m(22), 
				m(23).toLong, 
				m(24).toLong, 
				m(25).toLong,	// =>Int, 
				m(26).toLong,
				m(27).toLong, 
				m(28).toLong, 
				m(29).toLong,
        m(30).toFloat, 
				m(31).toFloat, 
				m(32).toFloat, 
				m(33).toFloat, 
				m(34), m(35),
        m(36), m(37), m(38), m(39).toLong, m(40), 
				m(41).toLong,
				m(42), m(43), "", m(44)
			))
			
			val df = sqlContext.createDataFrame(pcr).toDF()
			
			df.show()
			
			val df_2 = df.withColumn("flowendseconds", $"flowendseconds".cast("timestamp"))
			//val df_1 = df.withColumn("flowendseconds", $"flowendseconds".cast("timestamp"))
						 .withColumn("flowstartseconds", $"flowstartseconds".cast("timestamp"))
						 .withColumn("probe_id", $"probe_id".cast("int"))
						 .withColumn("destinationtransportport", $"destinationtransportport".cast("int"))
						 .withColumn("egressinterface", $"egressinterface".cast("int"))
						 .withColumn("ingressinterface", $"ingressinterface".cast("int"))
						 .withColumn("proceraexternalrtt", $"proceraexternalrtt".cast("int"))
						 .withColumn("procerahttpfilelength", $"procerahttpfilelength".cast("int"))
						 .withColumn("proceraresponsestatus", $"proceraresponsestatus".cast("int"))
						 .withColumn("procerainternalrtt", $"procerainternalrtt".cast("int"))
						 .withColumn("protocolidentifier", $"protocolidentifier".cast("int"))
						 .withColumn("sourcetransportport", $"sourcetransportport".cast("int"))		
			if(df_2.count > 0) {
				if(is_anonymize == "y" || is_anonymize == "Y")
				{
					val encryptedDF = df_2.withColumn("encrypted", encryptUDF('proceramsisdn))
					val encryptedNoMsisDnDF = encryptedDF.drop("proceramsisdn")	
					val encryptedMvDF = encryptedNoMsisDnDF.withColumnRenamed("encrypted",
													"proceramsisdn")
					encryptedMvDF.printSchema()
					encryptedMvDF.show()
					encryptedMvDF.groupBy("ld_date").count.show
					encryptedMvDF.write.format("parquet").mode(org.apache.spark.sql.SaveMode.Append).
							partitionBy("ld_date").
				  	  saveAsTable("procera_a") 
				}
				else {
					df_2.write.format("parquet").mode(org.apache.spark.sql.SaveMode.Append).
							partitionBy("ld_date").
							saveAsTable("procera_a") 
					df_2.printSchema()
					df_2.show()
					df_2.groupBy("ld_date").count.show
				}
			}
      
    } catch {
      case oob: java.lang.ArrayIndexOutOfBoundsException =>
        //oob.printStackTrace()
        System.out.println("ArrayIndexOutOfBoundException")
      case nfe: NumberFormatException =>
        System.out.println("NumberFormatException")
    }

  }
  
  def run(kafkaParams: Map[String, String], topics: Set[String], interval: Int,
          is_anonymize: String) {
    val conf = new SparkConf().setAppName("SparkProcera")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(interval.toInt))

    val sqlContext = new SQLContext(sc)
    
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val csv = messages.map(_._2).map(c => c.replaceAll("[\\r\\n]", "\0")).map(rdd => rdd.split(','))
    csv.foreachRDD(rdd => doAnonymize(sqlContext, rdd, is_anonymize))
    ssc.start()
    ssc.awaitTermination()
  }
  
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("\nUsage : DashboardCompute <topic-source> <topic-target> <interval> <anonymous,y/n>")
      System.err.println("Sample: DashboardCompute test-3 test55 5 y\n")
      System.exit(1)
    }

    val Array(topic_s, topic_d, interval, is_anonymize) = args

    // List of topics you want to listen for from Kafka
    val topics = List(topic_s).toSet

    val kafkaParams = Map[String, String](
      //"metadata.broker.list" -> "35.196.157.137:9092",
      //"metadata.broker.list" -> "172.16.2.110:9092,172.16.2.111:9092,172.16.2.112:9092",
      //"metadata.broker.list" -> "localhost:9092",
      //"metadata.broker.list" -> "cjkafdc01:9092,cjkafdc02:9092,cjkafdc03:9092",
      "metadata.broker.list" -> "172.16.2.130:9092,172.16.2.131:9092",
        "group.id" -> "spark_test" 
      //"auto.offset.reset" -> "largest"
    )

    System.out.println("Topic Source :" + topic_s)
    System.out.println("Topic Dest   :" + topic_d)
    System.out.println("Duration Time:" + interval + " sec.")
    System.out.println("Anonymous    :" + is_anonymize)

    run(kafkaParams, topics, interval.toInt, is_anonymize)

  }
}