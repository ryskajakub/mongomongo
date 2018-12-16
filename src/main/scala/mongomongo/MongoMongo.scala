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

case class Field[T](path: String)

case class NestedField[T](path: String)

case class JoinField[T](path: String) {
  def join = NestedField[T](path)
}

object JoinField

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

  trait Accs[T, U] {
    def unpack(t: T): (U, Seq[BsonField], Document)
  }

  implicit def OneAcc[T] = new Accs[Acc[T], Field[T]] {
    override def unpack(x: Acc[T]): (Field[T], Seq[BsonField], Document) =
      (x.mkField, Seq(x.bson), Document())
  }

  implicit def OneGroupId[T] = new Accs[Field[T], Field[T]] {
    override def unpack(t: Field[T]): (Field[T], Seq[BsonField], Document) = {
      (Field[T]("_id." + t.path), Seq.empty, Document(t.path -> ("$" + t.path)))
    }
  }

  implicit def TwoAcc[T, TT, U, UU](implicit t: Accs[T, TT], u: Accs[U, UU]) = new Accs[(T, U), (TT, UU)] {
    override def unpack(x: (T, U)): ((TT, UU), Seq[BsonField], Document) = {
      val tRe = t.unpack(x._1)
      val uRe = u.unpack(x._2)
      ((tRe._1, uRe._1), tRe._2 ++ uRe._2, tRe._3 ++ uRe._3)
    }
  }

  implicit def ThreeAcc[T, TT, U, UU, V, VV](implicit t: Accs[T, TT], u: Accs[U, UU], v: Accs[V, VV]) = new Accs[(T, U, V), (TT, UU, VV)] {
    override def unpack(x: (T, U, V)): ((TT, UU, VV), Seq[BsonField], Document) = {
      val tRe = t.unpack(x._1)
      val uRe = u.unpack(x._2)
      val vRe = v.unpack(x._3)
      ((tRe._1, uRe._1, vRe._1), tRe._2 ++ uRe._2 ++ vRe._2, tRe._3 ++ uRe._3 ++ vRe._3)
    }
  }

  trait Project[T] {
    def unpack(t: T): (T, Document)
  }

  implicit def OneProject[T] = new Project[Field[T]] {
    override def unpack(t: Field[T]): (Field[T], Document) = t -> Document(t.path -> 1)
  }

  implicit def TwoProject[T, U](implicit t1: Project[T], u1: Project[U]) = new Project[(T, U)] {
    override def unpack(t: (T, U)): ((T, U), Document) = {
      val r1 = t1.unpack(t._1)
      val r2 = u1.unpack(t._2)
      (r1._1 -> r2._1) -> (r1._2 ++ r2._2)
    }
  }

/*
  */

  def group[T, Xid, Y](q: Query[T], mkAccumulators: T => Xid)(implicit accs: Accs[Xid,Y]) : Query[Y] = {
    val accumulators = mkAccumulators(q.dbLevel)
    val (q2, bson, idBson) = accs.unpack(accumulators)
    val g = Aggregates.group(idBson, bson: _*)
    Query(q.bsonCommand ++ Seq(g), q2)
  }

  case class AccTuple[M[_] <: MkField, A, B](_1: M[A]#T, _2:M[B]#T)

  case class Acc[T](field: String, bson: BsonField) {
    def mkField: Field[T] = Field[T](field)
  }

  def sum(t: Field[Int]): Acc[Int] = Acc(t.path, Accumulators.sum(t.path, "$" + t.path))

  def first[A](t: Field[A]): Acc[A] = Acc(t.path, Accumulators.first(t.path, "$" + t.path))

  val machineQuery = mkQuery[Machine, DbLevel](machine)

  val usageGroup1 = group(
    machineQuery,
    (m: Machine[DbLevel]) => (sum(m.mth), m.company, first(m.`type`)),
  )

  def project[T, U](q: Query[T], f: T => U)(implicit pro: Project[U]): Query[U] = {
    val Query(b, d) = q
    val u = f(d)
    val projection = pro.unpack(u)
    Query[U](b ++ Seq(Aggregates.project(projection._2)), projection._1)
  }

  val usageGroup = project(usageGroup1, (a: (Field[Int], Field[Int], Field[String])) => a._2 -> a._3)

  def lookup[This, That <: Collection, This2, Key](c: This, ms: That, mkJoinedField: This => JoinField[That], mkLocalField: This => Field[Key], mkForeignField: That => Field[Key], copy: (This, NestedField[That]) => This2): Query[This2] = {
    val joinedField = mkJoinedField(c).join
    val cc = copy(c, joinedField)
    val pipeline = Seq(
      Aggregates.lookup(
        from = ms.collectionName,
        localField = mkLocalField(c).path,
        foreignField = mkForeignField(ms).path,
        as = joinedField.path,
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

  def getLast(path: String, doc: Document): (String, Document) = {
    val s = path.split('.')
    if (s.length == 1) s.head -> doc
    else getLast(s.tail.mkString("."), doc.apply[BsonDocument](s.head))
  }

  implicit val stringReader = new FieldReader[Field[String], String] {
    override def read(a: Field[String], doc: Document): String =
      doc.getString(a.path)
  }
  implicit val intReader = new FieldReader[Field[Int], Int] {
    override def read(a: Field[Int], doc: Document): Int = {
      val (s, doc2) = getLast(a.path, doc)
      doc2.getInteger(s).intValue
      //doc.getInteger(a.path).intValue
    }
  }
  implicit def joinedFieldReader[A,B](implicit rb: Reader[A,B]) = new FieldReader[NestedField[A], Seq[B]] {
    override def read(a: NestedField[A], doc: Document): Seq[B] = {
      val array = doc.apply[BsonArray](a.path)
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
    println(usageGroup.bsonCommand)
    println(usageGroup.dbLevel)
    for {
      results <- db.getCollection("machine").aggregate(usageGroup.bsonCommand).toFuture
    } yield {
      val reader = mkReaderT2(usageGroup.dbLevel)
      for {
        result <- results
      } yield {
        println(reader.read(result))
      }
    }
  }

}
