package com.ming.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * TODO
 *
 * @author Mingl lxm210787@gmail.com
 * @date 2022/9/14 - 9:39
 */

case class Rating(
                   userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Int
                 )

case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {

  // From MongoDB recommender.Rating
  val MONGODB_RATING_COLLECTION = "Rating"

  // To MongoDB
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

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

    // Load data from MongoDB
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .cache()

    // TODO
    // 1. 历史热门商品
    val rateMoreProductsDF = ratingDF.groupBy("productId")
      .agg(
        count($"userId").alias("count")
      ).orderBy(desc("count"))
    storeDFInMongoDb(rateMoreProductsDF, RATE_MORE_PRODUCTS)

    // 2. 近期热门商品
    val rateMoreRecentlyProductsDF: Dataset[Row] = ratingDF.withColumn(
      "year_month",
      date_format(from_unixtime($"timestamp"), "yyyyMM").cast(IntegerType)
    ).groupBy("year_month", "productId")
      .agg(
        count($"productId").alias("count")
      ).select("productId", "count", "year_month")
      .orderBy(desc("year_month"), desc("count"))
    storeDFInMongoDb(rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)

    // 3. 优质物品统计，根据商品的平均评分
    val averageProductsDF = ratingDF.groupBy("productId")
      .agg(
        avg("score").alias("avg")
      ).select("productId", "avg")
      .orderBy(desc("avg"))
    storeDFInMongoDb(averageProductsDF, AVERAGE_PRODUCTS)


    spark.stop()
  }

  def storeDFInMongoDb(df: DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig) = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collectionName)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()
  }
}
