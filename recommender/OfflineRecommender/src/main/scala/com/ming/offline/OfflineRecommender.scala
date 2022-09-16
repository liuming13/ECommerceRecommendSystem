package com.ming.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
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

    // TODO: 核心计算过程

    // 1. 训练隐语义模型
    val trainRDD = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    // rank 是模型中隐语义因子的个数, iterations 是迭代的次数, lambda 是ALS的正则化参数
    val (rank, iterations, lambda) = (50, 5, 0.01)

    // 2. 获取预测评分矩阵，得到用户的推荐列表
    val model = ALS.train(trainRDD, rank, iterations, lambda)

    // 计算用户推荐矩阵
    val userProducts = userRDD.cartesian(productRDD)
    val preRating = model.predict(userProducts)
    val userRecs = preRating
      .filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (userId, recs) =>
          UserRecs(userId, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => {
            Recommendation(x._1, x._2)
          }))
      }.toDF()

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 3. 利用商品特征向量，计算商品相似度列表
    // 获取商品的特征矩阵
    val productFeatures = model.productFeatures
    // 计算商品与商品的笛卡尔积
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter { case (a, b) => a._1 != b._1 }
      .map {
        case (a, b) =>
          val simScore = cosSimByArr(a._2, b._2)
          (a._1, (b._1, simScore))
      }
      //.filter(_._2._2 > 0.6)
      .groupByKey()
      .map {
        case (productId, items) =>
          ProductRecs(productId, items.toList.map(x => Recommendation(x._1, x._2)))
      }.toDF()

    println("productRecs-count: " + productRecs.count() + " " + userRDD.count() + " " + productRDD.count())
    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  def cosSimByArr(v1: Array[Double], v2: Array[Double]) = {
    val N = math.min(v1.length, v2.length)
    (0 until N).map(i => v1(i) * v2(i)).sum / math.sqrt((0 until N).map(i => v1(i) * v1(i)).sum * (0 until N).map(i => v2(i) * v2(i)).sum)
  }

  def cosSimByMap(m1: Map[Int, Double], m2: Map[Int, Double]) = {
    val intersectSet = m1.keySet.intersect(m2.keySet)
    val dotProduct = intersectSet.toArray.map(key => m1.get(key).get * m2.get(key).get).sum
    val mold = math.sqrt(m1.map(x => x._2 * x._2).sum * m2.map(x => x._2 * x._2).sum)
    val res = dotProduct / mold
    if (res.isNaN()) 0.0 else res
  }
}
