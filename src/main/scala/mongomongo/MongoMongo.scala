package mongomongo

import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.{BsonArray, BsonDocument}
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Aggregates

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.global

trait MkField {
  type T
  type Seq
  type Join
  type A[M] <: MkField
}

case class Field[T](name: String)

class JoinField[T]

case class JoinedField[T](name: String, elements: T)

trait DbLevel[TT] extends MkField {
  type T = Field[TT]
  type Seq = Field[TT]
  type Join
  type A[X] = DbLevel[X]
}

trait JoinLevel[TT] extends DbLevel[TT] {
  type Join = JoinedField[TT]
}

trait FlatLevel[TT] extends DbLevel[TT] {
  type Join = JoinField[TT]
}

trait AppLevel[TT] extends MkField {
  type T = TT
  type Seq = scala.collection.Seq[TT]
  type Join = scala.collection.Seq[TT]
  type A[X] = AppLevel[X]
}

case class Machine[M[_] <: MkField](`type`: M[String]#T, mth: M[Int]#T, company: M[Int]#T)

case class Company[M[_] <: MkField](id: M[Int]#T, name: M[String]#T, machines: M[Machine[M[Nothing]#A]]#Join)

object MongoMongo {

  implicit val ec = global

  val machine = Machine[DbLevel](Field("type"), Field("mth"), Field("company"))

  type FlatC = Company[FlatLevel]
  type MS = JoinedField[Machine[DbLevel]]
  type JoinC = Company[JoinLevel]

  val company = Company[FlatLevel](Field("id"), Field("name"), new JoinField)

  //val company2 = Company[DbLevel](Field("id"), Field("name"), new JoinedField("no-name", machine))

  val cAppLevelTest = Company[AppLevel](1, "2e plus", Seq(Machine[AppLevel]("BK30", 3, 1)))

  case class Query[T](bsonCommand: Seq[Bson], dbLevel: T)

  def lookup[C, MS, CC](c: C, ms: MS, copy: (C, JoinedField[MS]) => CC): Query[CC] = {
    val cc = copy(c, new JoinedField("machines", ms))
    val pipeline = Seq(
      Aggregates.lookup(
        from = "machine",
        localField = "id",
        foreignField = "company",
        as = "machines"
      )
    )
    Query(
      pipeline,
      cc
    )
  }

  trait Reader[A, B] {
    def read(doc: Document): B
  }

  trait FieldReader[A, B] {
    def read(a: A, doc: Document): B
  }

  implicit val stringReader = new FieldReader[Field[String], String] {
    override def read(a: Field[String], doc: Document): String = doc.getString(a.name)
  }
  implicit val intReader = new FieldReader[Field[Int], Int] {
    override def read(a: Field[Int], doc: Document): Int = {
      doc.getInteger(a.name).intValue
    }
  }
  implicit def joinedFieldReader[A,B](implicit rb: Reader[A,B]) = new FieldReader[JoinedField[A], Seq[B]] {
    override def read(a: JoinedField[A], doc: Document): Seq[B] = {
      val array = doc.apply[BsonArray](a.name)
      val x = array.asScala.map(bson => rb.read(bson.asDocument))
      x
    }
  }

  def mkReader3[T[_[_]], A, B, C, A1, B1, C1](caseClassTuple: (A1, B1, C1), constructor: (A, B, C) => T[AppLevel])(
      implicit a: FieldReader[A1, A], b: FieldReader[B1, B], c: FieldReader[C1, C]) = new Reader[T[DbLevel], T[AppLevel]] {
    override def read(doc: Document): T[AppLevel] = {
      val aa = a.read(caseClassTuple._1, doc)
      val bb = b.read(caseClassTuple._2, doc)
      val cc = c.read(caseClassTuple._3, doc)
      constructor(aa, bb, cc)
    }
  }

  implicit val machineReader = mkReader3[Machine, String, Int, Int, Field[String], Field[Int], Field[Int]](
    Machine.unapply(machine).get, Machine.apply[AppLevel])

  val usage: Query[JoinC] = lookup/*[Company[FlatLevel], Machine[DbLevel], Company[JoinLevel]]*/(
    company, machine, (cp: Company[FlatLevel], jf: JoinedField[Machine[DbLevel]]) => cp.copy[JoinLevel](machines = jf))

/*
  def read[A, B](q: Query[A])(implicit ev: Reader[A, B]): B = {
    ev.read(q.dbLevel, q.bsonCommand)
  }
  */

  //val cAppLevel: Machine[AppLevel] = read(query)

  val companyReader = mkReader3[Company, Int, String, Seq[Machine[AppLevel]], Field[Int], Field[String], JoinedField[Machine[DbLevel]]](
    Company.unapply(usage.dbLevel).get, Company.apply[AppLevel])

  def runCommand(db: MongoDatabase) = {
    for {
      results <- db.getCollection("company").aggregate(usage.bsonCommand).toFuture
    } yield {

      println(usage.bsonCommand)
      println(results)

      for {
        result <- results
      } yield {
        val read = companyReader.read(result)
        println(read)
      }
    }
  }

}
