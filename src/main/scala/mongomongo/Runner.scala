package mongomongo

import org.mongodb.scala.MongoClient

import scala.concurrent.Await
import scala.concurrent.duration._

object Runner {

  def main (args: Array[String]) = {
    val mc = MongoClient()

    val db = mc.getDatabase("optimus-maximus-prod")

    val r = MongoMongo.runCommand(db)

    Await.result(r, 20 seconds)

    ()

  }

}
