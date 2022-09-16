package com.ming.online

import com.mongodb.casbah.Imports.{MongoClient, MongoDBObject}
import com.mongodb.casbah.{MongoClientURI, MongoCollection}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * TODO
 *
 * @author Mingl lxm210787@gmail.com
 * @date 2022/9/15 - 19:33
 */
//
object ConnHelper extends Serializable {
  //
  lazy val jedis = new Jedis("master")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://master:27017/recommender"))
}

case class MongoConfig(uri: String, db: String)

// 定义标准推荐对象
case class Recommendation(product: Int, score: Double)

// 定义用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 定义商品相似度列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object OnlineRecommender {
  //
  val MONGODB_RATING_COLLECTION = "Rating"
  val STREAM_RECS = "StreamRecs"
  val PRODUCT_RECS = "ProductRecs"
  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://master:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    val sparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .setAppName(this.getClass.getName)

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据（相似度矩阵）
    val simProductMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      .map { item =>
        (item.productId, item.recs.map(x => (x.product, x.score)).toMap)
      }
      .collectAsMap()

    println("size => " + simProductMatrix.size)

    val simProductMatrixBC = sc.broadcast(simProductMatrix)

    //
    ssc.checkpoint("./spark-receiver")

    val kafkaParams = Map(
      "bootstrap.servers" -> "master:9092",
      "group.id" -> "spark-recommender",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer]
    )

    val topics = Set("recommender")

    val kafkaDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    val ratingStream = kafkaDStream.map { msg =>
      var attr = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    ratingStream.foreachRDD {
      rdd =>
        rdd.foreach {
          case (userId, productId, score, timestamp) =>
            println("rating data coming!>>>>>>>>>>>>>>>>>>>")
            // TODO: core al
            // 1. 从Redis中取出当前用户的最近评分
            val userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATING_NUM, userId, ConnHelper.jedis)

            // 2. 从相似度矩阵中获取当前商品最相似的商品列表，作为备选列表
            val candidateProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, productId, userId, simProductMatrixBC.value)

            // 3. 计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表
            val streamRecs = computeProductScore(candidateProducts, userRecentlyRatings, simProductMatrixBC.value)

            // 4. 把推荐列表保存到MongoDB
            saveDataToMongoDB(userId, streamRecs)
        }
    }

    ssc.start()
    println("Streaming started!")
    ssc.awaitTermination()
  }

  import scala.collection.JavaConversions._

  def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis) = {
    //
    jedis.lrange("userId:" + userId.toString, 0, num)
      .map { item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  def getTopSimProducts(num: Int, productId: Int, userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig) = {
    println(222222222)

    //
    val allSimProducts = simProducts(productId).toArray
    println(allSimProducts)
    //
    val ratingCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
    val ratingExist = ratingCollection.find(MongoDBObject("userId" -> userId)).toArray
      .map { item =>
        item.get("productId").toString.toInt
      }
    //
    val productIds: Array[Int] = allSimProducts.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2)
      .take(num)
      .map(_._1)
    productIds
  }

  //
  def computeProductScore(candidateProducts: Array[Int],
                          userRecentlyRatings: Array[(Int, Double)],
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]) = {
    println(33333333)
    //
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    val incrementMap = scala.collection.mutable.HashMap[Int, Int]()
    val decrementMap = scala.collection.mutable.HashMap[Int, Int]()

    //
    for (candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings) {
      val simScore = getProductsSimScore(candidateProduct, userRecentlyRating._1, simProducts)
      if (simScore > 0.4) {
        scores += ((candidateProduct, simScore * userRecentlyRating._2))
      }
      if (userRecentlyRating._2 > 3) {
        incrementMap(candidateProduct) = incrementMap.getOrDefault(candidateProduct, 0) + 1
      } else {
        decrementMap(candidateProduct) = decrementMap.getOrDefault(candidateProduct, 0) + 1
      }
    }

    // 根据公式计算所有的推荐的优先级
    scores.groupBy(_._1).map {
      case (productId, scoreList) =>
        (productId,
          scoreList.map(_._2).sum / scoreList.length +
            log(incrementMap.getOrDefault(productId, 1)) -
              log(incrementMap.getOrDefault(productId, 1)))
    }
      //
      .toArray
      .sortWith(_._2 > _._2)
  }

  def getProductsSimScore(product1: Int, product2: Int,
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]) = {
    simProducts.get(product1) match {
      case Some(sims) => sims.get(product2) match {
        case Some(value) => value
        case None => 0.0
      }
      case None => 0.0
    }
  }

  def log(m: Int): Double = {
    val N = 10
    math.log(m) / math.log(N)
  }

  def saveDataToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig) = {
    val streamRecsCollection: MongoCollection = ConnHelper.mongoClient(mongoConfig.db)(STREAM_RECS)
    //
    streamRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streamRecsCollection.insert(
      MongoDBObject("userId" -> userId, "recs" -> streamRecs.map(x => MongoDBObject("productId" -> x._1, "score" -> x._2)))
    )
  }
}
