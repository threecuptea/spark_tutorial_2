package org.freemind.spark.sql

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Rating(userId: Int, movieId: Int, rating: Float)
case class Movie(id: Int, title: String, genres: Array[String])

object MovieLensALSColdStart {

  def parseRating(line: String): Rating = {
    val splits = line.split("::")
    assert( splits.length == 4)
    Rating(splits(0).toInt, splits(1).toInt, splits(2).toFloat)
  }

  def parseMovie(line: String): Movie = {
    val splits = line.split("::")
    assert( splits.length == 3)
    Movie(splits(0).toInt, splits(1), splits(2).split("|"))
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: MovieLensALSColdStart [movie_ratings] [personal_ratings] [movies]")
      System.exit(-1)
    }

    val spark = SparkSession.builder().appName("MovieLensALSColdStart").getOrCreate()

    import spark.implicits._

    val mrFile = args(0)
    val prFile = args(1)
    val movieFile = args(2)

    val mrDs = spark.read.textFile(mrFile).map(parseRating).cache()
    val prDs = spark.read.textFile(prFile).map(parseRating).cache()
    val movieDs = spark.read.textFile(prFile).map(parseMovie).cache()
    mrDs.show(10, false)
    println(s"Rating Counts: movie - ${mrDs.count}, personal - ${prDs.count}")
    movieDs.show(10, false)
    println(s"Movie Count: ${movieDs.count}")

    val Array(trainDS, valDS, testDS) = mrDs.randomSplit(Array(0.8, 0.1, 0.1))
    trainDS.cache()
    valDS.cache()
    val total = trainDS.count() + valDS.count() + testDS.count()
    println(s"Total Sums of splits: ${total}")

    val trainPlusDS = trainDS.union(prDs).cache()
    val allDS = mrDs.union(prDs).cache()

    //Initialize evaluator
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
    val avgRating = trainDS.select(mean($"rating")).first().getDouble(0) //prediction requires double
    val baselineRmse = spark.sparkContext.broadcast(evaluator.evaluate(testDS.withColumn("prediction", lit(avgRating))))
    printf("The baseline rmse= %3.2f.\n", baselineRmse.value)
    println()

    /**
      * ALS codes starts here
      * ALS (alternating least square is a popular model that spark-ml use for 'Collaborative filtering'
      * KEY POINT is coldStartStrategy = "drop"
      */
    val als = new ALS()
      .setMaxIter(20).setColdStartStrategy("drop").setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

    val ranks = Array(10, 12)  //numbers of latent factor used to predict missing entries of user-item matrics, the default is 10
    val regParams = Array(0.8, 0.5, 0.1) //the default is 1.0

    val paramGrids = new ParamGridBuilder()
      .addGrid(als.regParam, regParams)
      .addGrid(als.rank, ranks)
      .build()

    var bestModel: Option[ALSModel] = None
    var bestRmse = Double.MaxValue

    val models: Seq[ALSModel] = als.fit(trainPlusDS, paramGrids)
    for (model <- models) {
      val prediction = model.transform(valDS)
      val rmse = evaluator.evaluate(prediction)
      if (rmse < bestRmse) {
        bestRmse = rmse
        bestModel = Some(model)
      }
    }

    bestModel match {
      case None =>
        println("Unable to find a good ALSModel.  STOP here")
        System.exit(-1)
      case Some(model) =>
        val prediction = model.transform(testDS)
        val rmse = evaluator.evaluate(prediction)
        val improvement = (baselineRmse.value - rmse) / baselineRmse.value * 100
        println(s"The best model from ALS was trained with param = ${model.extractParamMap}")
        printf("The RMSE of the bestModel from ALS on test set is %3.2f, which is %3.2f%% over baseline.\n", rmse, improvement)
        println()
    }

    val bestParamFromALS = bestModel.get.extractParamMap()
    val augModelFromALS = als.fit(allDS, bestParamFromALS)  //Refit.  Is it necessary

    val Array(cvTrainDS, cvTestDS) = mrDs.randomSplit(Array(0.9, 0.1))

    val cv = new CrossValidator()
      .setEstimator(als).setEvaluator(evaluator).setEstimatorParamMaps(paramGrids).setNumFolds(10)  // percentage close to 0.8, 0.1, 0.1

    val cvBestModel = cv.fit(cvTrainDS)
    val cvPrediction = cvBestModel.transform(cvTestDS)
    val cvRmse = evaluator.evaluate(cvPrediction)
    println(s"The best model from CrossValidator was trained with param = ${cvBestModel.extractParamMap}")
    printf("The RMSE of the bestModel from CrossValidator on test set is %3.2f\n", cvRmse)
    println()

    val augModelFromCV = cv.fit(allDS, cvBestModel.extractParamMap())

    //manual way
    val pMovieId = prDs.map(_.movieId).collect()
    val pUserId = 0
    //So that I can match ALS column name and also have movie title
    val unRatedDS = movieDs.filter(mv => !pMovieId.contains(mv.id)).withColumnRenamed("id", "movieId").withColumn("userId", lit(pUserId))
    //Using ALS manual
    println("The recommendation for unratedMovie for user 0 from ALS model")
    augModelFromALS.transform(unRatedDS).sort(desc("prediction")).show(25, false)
    println("The recommendation for unratedMovie for user 0 from CrossValidator with ALS as estimator")
    augModelFromCV.transform(unRatedDS).sort(desc("prediction")).show(25, false)

    augModelFromALS.recommendForAllUsers(25).filter($"userId" === 0).show(false)
    augModelFromCV.bestModel.asInstanceOf[ALSModel].recommendForAllUsers(25).filter($"userId" === 0).show(false)

  }
}
