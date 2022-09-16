package com.ming.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}

/**
 * TODO
 *
 * @author Mingl lxm210787@gmail.com
 * @date 2022/9/15 - 19:11
 */
object ALSTrainer {

  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.10.10:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf: SparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .setAppName(this.getClass.getName)

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载评分数据
    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => Rating(rating.userId, rating.productId, rating.score)).cache()

    // 切分训练和预测数据集
    val splits = ratingRDD.randomSplit(Array(0.7, 0.3))
    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    // 输出最优参数
    adjustALSParams(trainingRDD, testingRDD)


    spark.stop()
  }

  def adjustALSParams(trainingRDD: RDD[Rating], testingRDD: RDD[Rating]) = {
    val result = for (rank <- Array(100, 200, 250); lambda <- Array(1, 0.1, 0.01, 0.001))
      yield {
        val model = ALS.train(trainingRDD, rank, 5, lambda)
        val errorByRMSE = computeRMSE(model, testingRDD)
        (rank, lambda, errorByRMSE)
      }
    println(result.sortBy(_._3).head)
  }

  def computeRMSE(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)
    val real = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => {
      ((item.user, item.product), item.rating)
    })

    // 计算 RMSE
    math.sqrt(
      real.join(predict).map {
        case ((userId, productId), (real, pre)) =>
          val err = real - pre
          err * err
      }.mean()
    )
  }
}