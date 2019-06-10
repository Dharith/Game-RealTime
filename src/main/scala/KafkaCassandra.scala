import java.util
import java.util.Properties

import com.datastax.spark.connector._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._


object KafkaCassandra {

  def main(args: Array[String]) {

    //This will just fetch the Row count in Table "game_transaction_by_upline"

    System.setProperty("hadoop.home.dir", "C:\\Users\\m.dharini\\Documents\\winutils\\")

    val conf = new SparkConf().setAppName("Game-RealTime").setMaster("local")
                    .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    conf.set("spark.cassandra.connection.host", "54.169.77.149")
    conf.set("spark.cassandra.connection.port", "9042")

    val sc = new SparkContext(conf)
    val rdd = sc.cassandraTable("slotty", "game_transaction_by_upline")
    val num_row = rdd.count()
    println("Number of rows in system_schema.slotty: " + num_row + "nn")

    val sql = new SQLContext(sc)
    val gameDF= sql.read.json("C:\\Users\\m.dharini\\Documents\\Game-RealTime\\src\\main\\scala\\gameData.json")
    gameDF.printSchema()
    gameDF.show()
   // gameDF.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "winloss", "keyspace" -> "slotty")).save()
    //val ssc = new StreamingContext(conf, Seconds(1))

    //get a json Record from the API and stream to Kafka
    //write json to the Table "winloss"

//    val rows = "SELECT * From slotty.game_transaction_by_upline LIMIT 3"
//
//    val spark: SparkSession = SparkSession
//      .builder
//      .appName("Game-RealTime")
//      .master("local")
//      .getOrCreate
//
//    val jsonS = """{"upline" :"uplineeg","halfdate" : "2019-5-29", "currency":"$","bet_type_ocode":"ocode","user_type":"expert","user_level":"3","username":"name"}"""
//
//    val people = spark.read.json(jsonS)
//    people.printSchema()
//   val sqlContext = new SQLContext(sc)
//    val json = sc.parallelize(Seq("""{"user":"helena","commits":98, "month":12, "year":2014}""","""{"user":"pkolaczk", "commits":42, "month":12, "year":2014}"""))
//
//    sqlContext.jsonRDD(json).map(MonthlyCommits(_)).saveToCassandra("githubstats","monthly_commits")
//
//    sc.cassandraTable[MonthlyCommits]("githubstats","monthly_commits").collect foreach println
    //case class model(upline: String, halfdate: String,currency:String,bet_type_ocode:String,user_type:String, user_level:String, username:String)

//    val collection = sc.parallelize(Seq("uplineeg","2019-5-29", "$","ocode","expert","3","name"))
//    collection.saveToCassandra("slotty", "winloss", SomeColumns("upline", "halfdate","currency","bet_type_ocode","user_type","user_level","username"))

    writeToKafka("gameTransaction")
    consumeFromKafka("gameTransaction")
    sc.stop

  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "kafka1.joker.local:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, "key3", """{"upline":"uplineeg","halfdate":"2019-5-29","currency":"$","bet_type_ocode":"ocode","user_type":"expert","user_level":"3","username":"name"}""")
    producer.send(record)
    producer.close()
  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "kafka1.joker.local:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "consumer-group")
    props.put("enable.auto.commit", "true")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singletonList(topic))

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(100)
      //records.iterator().asScala
      //records.records(topic).iterator()..foreach{ record =>
        //println(s"Received : ${record.toString}")}
      //val record = consumer.poll(1000)
      for(recordC <- records.iterator()){
        println(recordC.value())
    }
  }
}
}