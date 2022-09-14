package com.ming.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * TODO
 *
 * @author Mingl lxm210787@gmail.com
 * @date 2022/9/14 - 7:39
 */

/**
 *
 * @param productId
 * @param name
 * @param imageUrl
 * @param categories
 * @param tags
 */
case class Product(
                    productId: Int,
                    name: String,
                    imageUrl: String,
                    categories: String,
                    tags: String
                  )

/**
 *
 * @param userId
 * @param productId
 * @param score
 * @param timestamp
 */
case class Rating(
                   userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Int
                 )

/**
 *
 * @param uri
 * @param db
 */
case class MongoConfig(uri: String, db: String)

object DataLoader {

  val PRODUCT_DATA_PATH = "D:\\Program Files\\decompiler-java\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "D:\\Program Files\\decompiler-java\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"

  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

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

    val productRDD: RDD[String] = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map(item => {
      val attr = item.split("\\^")
      Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    storeDataInMongoDB(productDF, ratingDF)

    spark.stop()
  }

  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig) = {
    //
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    //
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    // create index
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))

    mongoClient.close()
  }
}


