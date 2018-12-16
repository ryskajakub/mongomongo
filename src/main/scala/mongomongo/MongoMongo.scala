package mongomongo

import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.{BsonArray, BsonDocument, BsonNull}
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.{Accumulators, Aggregates, BsonField}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.global

trait MkField {
  type T
  type Seq
  type Join
  type A[M] <: MkField
}

case class Field[T](name: String)

case class NestedField[T](name: String)

case class JoinField[T](name: String) {
  def join = NestedField[T](name)
}

case class AccumulatorField[T](name: String)

trait DbLevel[TT] extends MkField {
  type T = Field[TT]
  type Seq = Field[TT]
  type Join
  type A[X] = DbLevel[X]
}

trait NestedLevel[TT] extends DbLevel[TT] {
  type Join = NestedField[TT]
}

trait JoinLevel[TT] extends DbLevel[TT] {
  type Join = JoinField[TT]
}

trait Accumulators2[TT] extends MkField {
  type T = AccumulatorField[TT]
}

trait AppLevel[TT] extends MkField {
  type T = TT
  type Seq = scala.collection.Seq[TT]
  type Join = scala.collection.Seq[TT]
  type A[X] = AppLevel[X]
}

trait Collection {
  def collectionName: String
}

case class Machine[M[_] <: MkField](`type`: M[String]#T, mth: M[Int]#T, company: M[Int]#T) extends Collection {
  def collectionName: String = "machine"
}

case class Company[M[_] <: MkField](id: M[Int]#T, name: M[String]#T, machines: M[Machine[M[Nothing]#A]]#Join) extends Collection {
  def collectionName: String = "company"
}

object MongoMongo {

  implicit val ec = global

  val machine = Machine[DbLevel](Field("type"), Field("mth"), Field("company"))

  type FlatC = Company[JoinLevel]
  type MS = NestedField[Machine[DbLevel]]
  type JoinC = Company[NestedLevel]

  val company = Company[JoinLevel](Field("id"), Field("name"), JoinField("machines"))

  //val company2 = Company[DbLevel](Field("id"), Field("name"), new JoinedField("no-name", machine))

  val cAppLevelTest = Company[AppLevel](1, "2e plus", Seq(Machine[AppLevel]("BK30", 3, 1)))

  case class Query[T](bsonCommand: Seq[Bson], dbLevel: T)

  def mkQuery[A[_[_]], B[_] <: DbLevel[_]](a: A[B]) = Query(Seq.empty, a)

  trait PickAcc[A] {
    def accs(a: A): Seq[AccumulatorField[_]]
  }

  type T1[M[_] <: DbLevel[_], A] = Tuple1[M[A]#T]
  type T2[M[_] <: DbLevel[_], A, B] = (M[A]#T, M[B]#T)

  def group[T, X, Q](q: Query[T], mkAccumulators: T => X, reader: X => (Q, Seq[BsonField])) : Query[Q] = {
    val accumulators = mkAccumulators(q.dbLevel)
    val (q2, bson) = reader(accumulators)
    val g = Aggregates.group(BsonNull(), bson: _*)
    Query(q.bsonCommand ++ Seq(g), q2)
  }

  case class AccTuple[M[_] <: MkField, A, B](_1: M[A]#T, _2:M[B]#T)

  case class Acc[T](field: String, bson: BsonField) {
    def mkField: Field[T] = Field[T](field)
  }

  def sum(t: Field[Int]): Acc[Int] = Acc(t.name, Accumulators.sum(t.name, "$" + t.name))

  def first[A](t: Field[A]): Acc[A] = Acc(t.name, Accumulators.first(t.name, "$" + t.name))

  val machineQuery = mkQuery[Machine, DbLevel](machine)

  val usageGroup = group(
    machineQuery,
    (m: Machine[DbLevel]) => sum(m.mth) -> first(m.`type`),
    (arg: (Acc[Int], Acc[String])) => (arg._1.mkField -> arg._2.mkField) -> Seq(arg._1.bson, arg._2.bson)
  )

  def lookup[This, That <: Collection, This2, Key](c: This, ms: That, mkJoinedField: This => JoinField[That], mkLocalField: This => Field[Key], mkForeignField: That => Field[Key], copy: (This, NestedField[That]) => This2): Query[This2] = {
    val joinedField = mkJoinedField(c).join
    val cc = copy(c, joinedField)
    val pipeline = Seq(
      Aggregates.lookup(
        from = ms.collectionName,
        localField = mkLocalField(c).name,
        foreignField = mkForeignField(ms).name,
        as = joinedField.name,
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
  implicit def joinedFieldReader[A,B](implicit rb: Reader[A,B]) = new FieldReader[NestedField[A], Seq[B]] {
    override def read(a: NestedField[A], doc: Document): Seq[B] = {
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

  implicit def mkReaderT2[A, B](tuple: (Field[A], Field[B]))(
    implicit a: FieldReader[Field[A], A], b: FieldReader[Field[B], B]) =
      new Reader[(Field[A], Field[B]), (A, B)] {
        override def read(doc: Document): (A, B) = {
          val aa = a.read(tuple._1, doc)
          val bb = b.read(tuple._2, doc)
          (aa, bb)
        }
      }

  implicit val machineReader = mkReader3/*[Machine, String, Int, Int, Field[String], Field[Int], Field[Int]]*/(
    Machine.unapply(machine).get, Machine.apply[AppLevel])

  val usage: Query[JoinC] = lookup/*[Company[FlatLevel], Machine[DbLevel], Company[JoinLevel]]*/(
    company,
    machine,
    (cp: Company[JoinLevel]) => cp.machines,
    (cp: Company[JoinLevel]) => cp.id,
    (cp: Machine[DbLevel]) => cp.company,
    (cp: Company[JoinLevel], jf: NestedField[Machine[DbLevel]]) => cp.copy[NestedLevel](machines = jf))

  val companyReader = mkReader3/*[Company, Int, String, Seq[Machine[AppLevel]], Field[Int], Field[String], NestedField[Machine[DbLevel]]]*/(
    Company.unapply(usage.dbLevel).get, Company.apply[AppLevel])

  def runCommand(db: MongoDatabase) = {
    for {
      results <- db.getCollection("machine").aggregate(usageGroup.bsonCommand).toFuture
    } yield {

      println(usageGroup.bsonCommand)
      val reader = mkReaderT2(usageGroup.dbLevel)

      for {
        result <- results
      } yield {

        val read = reader.read(result)

        println(read)

      }
    }
  }

}
