package com.trueit.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext

object SparkProceraCompute {
  def main(args: Array[String]) 
	{
    if (args.length < 3) {
			System.err.println("\nUsage : DashboardCompute <topic-source> <topic-target> <interval> <anonymous,y/n>")
			System.err.println("Sample: DashboardCompute test-3 test55 5 y\n")
			System.exit(1)
		}
    
    val Array(topic_s, topic_d, interval, is_anonymous) = args

		System.out.println("Topic Source :" + topic_s)
		System.out.println("Topic Dest   :" + topic_d)
		System.out.println("Duration Time:" + interval + " sec.")
		System.out.println("Anonymous    :" + is_anonymous)
		
		val conf = new SparkConf().setAppName("SparkProcera")
		
		val sc = new SparkContext(conf)
    
    val ssc = new StreamingContext(sc, Seconds(interval.toInt))
    
    //val sqlContext = new SQLContext(sc)
    
    // List of topics you want to listen for from Kafka
		val topics = List(topic_s).toSet

		val kafkaParams = Map[String, String](
      //"metadata.broker.list" -> "35.196.157.137:9092",
      //"metadata.broker.list" -> "172.16.2.110:9092,172.16.2.111:9092,172.16.2.112:9092",
      "metadata.broker.list" -> "localhost:9092",
		  //"metadata.broker.list" -> "cjkafdc01:9092,cjkafdc02:9092,cjkafdc03:9092",
      "group.id" -> "spark_test"
      //"auto.offset.reset" -> "largest"
    )

		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder,
							StringDecoder](ssc, kafkaParams, topics)
							
		val csv = messages.map(_._2).map(c=>c.replaceAll("[\\r\\n]", "\0")).map(rdd=>rdd.split(','))
		
		csv.foreachRDD( rdd => {
						rdd.collect.foreach( record => {
						      println(record(0) + "," + record(1) + "," + record(2) + ":" + record)
						})    
    })
			
			
    ssc.start()
    ssc.awaitTermination()
		
	}
}