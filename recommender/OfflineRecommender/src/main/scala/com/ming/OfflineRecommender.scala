package com.ming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * TODO
 *
 * @author Mingl lxm210787@gmail.com
 * @date 2022/9/14 - 10:28
 */
case class ProductRating(
                   userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Int
                 )

case class MongoConfig(uri: String, db: String)

// 定义标准推荐对象
case class Recommendation(product: Int, score: Double)

// 定义用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 定义商品相似度列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object OfflineRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://master:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf: SparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .setAppName(this.getClass.getName)

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))


    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => (rating.userId, rating.productId, rating.score))
      .cache()

    // Get all userId
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()

    println(userRDD.count())
    println(productRDD.count())

    // TODO: 核心计算过程

    // 1. 训练隐语义模型
    val trainRDD = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    // 2. 获取预测评分矩阵，得到用户的推荐列表

    // 3. 利用商品特征向量，计算商品相似度列表

    spark.stop()
  }

}
