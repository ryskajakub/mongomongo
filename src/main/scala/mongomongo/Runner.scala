package mongomongo

import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.collection.immutable.Document

import scala.concurrent.Await
import scala.concurrent.duration._

object Runner {

  def main (args: Array[String]) = {
    val mc = MongoClient()

    val db = mc.getDatabase("optimus-maximus-prod")

    //val r = MongoMongo.runCommand(db)

    val r = db.runCommand(Document(
      "aggregate" -> "machine",
      "pipeline" -> Seq(
        Document(
          "$project" -> Document(
            "company" -> 1
          )
        )
      ),
      "cursor" -> Document()
    )).toFuture()

    val res = Await.result(r, 20 seconds)

    println(res)

    ()

  }

}
