package org.freemind.spark.sql



case class Rating(userId: Int, movieId: Int, rating: Float)
case class Movie(id: Int, title: String, genres: Array[String])

class MovieLensParser extends Serializable {

  def parseRating(line: String): Rating = {
    val splits = line.split("::")
    assert(splits.length == 4)
    Rating(splits(0).toInt, splits(1).toInt, splits(2).toFloat)
  }


// If you use quotes, you're asking for a regular expression split.  | is the "or" character, so your regex matches nothing or nothing. So everything is split.
// If you use split('|') or split("""\|""") you should get what you want.
  def parseMovie(line: String): Movie = {
    val splits = line.split("::")
    assert(splits.length == 3)
    Movie(splits(0).toInt, splits(1), splits(2).split('|'))
  }

}
