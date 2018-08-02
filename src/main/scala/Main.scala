
import com.twitter.concurrent.AsyncMeter
import com.twitter.finagle.filter.RequestMeterFilter
import com.twitter.finagle.http.{Request, RequestBuilder, Response}
import com.twitter.finagle.{Http, SimpleFilter}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Await, Future}
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import Futures._

object Main extends App {
  // Used to decode JSON responses
  case class Actor(id: Int, name: String)
  case class MovieTvShowCredits(id: Int, cast: List[Actor])
  case class MovieTvShow(id: Int)
  case class DiscoverResponse(page: Int, total_results: Int, total_pages: Int, results: List[MovieTvShow])

  private val apiKey = "===API Key==="
  // TODO: Use joda library's DateTime to be typesafe
  private val startDate = "2017-12-01"
  private val endDate = "2017-12-31"

  private val host = "api.themoviedb.org"
  private val port = "80"

  val client = {
    val client = Http.client.newService(s"$host:$port")

    // Limit requests to 3 per second to comply with themoviedb.org's limit
    implicit val timer = DefaultTimer.twitter
    val meter = AsyncMeter.perSecond(3, 1000)
    val requestMeterFilter: SimpleFilter[Request, Response] = new RequestMeterFilter(meter)

    requestMeterFilter andThen client
  }

  def getMoviesOrTvShowsF(page: Int, movieFlag: Boolean): Future[DiscoverResponse] = {
    val subUrl = {
      if (movieFlag) {
        s"movie?release_date.gte=$startDate&release_date.lte=$endDate&api_key=$apiKey&page=$page"
      } else {
        s"tv?air_date.gte=$startDate&air_date.lte=$endDate&api_key=$apiKey&page=$page"
      }
    }

    val requestF = client(
      RequestBuilder()
        .url(s"http://$host/3/discover/$subUrl")
        .buildGet()
    )

    for {
      response <- requestF
    } yield {
      decode[DiscoverResponse](response.contentString) match {
        case Right(genResponse) => genResponse
        case Left(error) => throw new Exception(s"Error while trying to decode response ${response.contentString}")
      }
    }
  }

  def getMoviesOrTvShowCreditsF(id: Int, movieFlag: Boolean): Future[Either[String, MovieTvShowCredits]] = {
    val entertainmentType = if (movieFlag) "movie" else "tv"

    val requestF = client(
      RequestBuilder()
        .url(s"http://$host/3/$entertainmentType/$id/credits?api_key=$apiKey")
        .buildGet()
    )

    for {
      response <- requestF
    } yield {
      decode[MovieTvShowCredits](response.contentString) match {
        case Right(genResponse) => Right(genResponse)
        case Left(error) => Left(s"Exception occurred when trying to get credit for $entertainmentType id $id: $response")
      }
    }
  }

  def getAllTvOrMovieActorIdsF(movieFlag: Boolean): Future[Set[Int]] = {
    for {
      firstPage <- getMoviesOrTvShowsF(1, movieFlag)
      totalPages = firstPage.total_pages

      restOfPages <- Futures.groupedCollect((2 to totalPages), 4)(page => getMoviesOrTvShowsF(page, movieFlag))

      movieOrTvs = firstPage.results ++ restOfPages.flatMap(_.results)

      movieOrTvIds = movieOrTvs.map(_.id).toSet

      allCredits <- Futures.groupedCollect(movieOrTvIds, 4)(movieOrTvId => getMoviesOrTvShowCreditsF(movieOrTvId, movieFlag))

      // TODO: Figure out what to do with failed responses (lefts)
      (lefts, rights) = allCredits.partition(_.isLeft)
    } yield {
      rights
        .map(_.right.get)
        .flatMap(_.cast)
        .map(_.id)
        .toSet
    }
  }

  Await.result(for {
    allMovieActorIds <- getAllTvOrMovieActorIdsF(movieFlag = true)

    allTvActorIds <- getAllTvOrMovieActorIdsF(movieFlag = false)

    allMovieAndTvActors = allMovieActorIds.filter(allTvActorIds.contains(_))
  } yield {
    // TODO: Return a better format
    println(s"allMovieActorIds.size: ${allMovieActorIds.size}")
    println(s"allTvActorIds.size: ${allTvActorIds.size}")
    println(s"allMovieAndTvActors.size: ${allMovieAndTvActors.size}")
  })
}
