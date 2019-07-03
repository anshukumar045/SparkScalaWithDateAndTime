import scala.util._
import java.time._
import java.time.format._

trait FromString[A] {
  def fromString(string: String): A
}
object FromString {
    implicit object StrToDouble extends FromString[Double] {
        def fromString(string: String): Double = string.toDouble
    }
    implicit object StrToInt extends FromString[Int] {
        override def fromString(string: String): Int =   string.toInt
    }
    implicit object StrToStr extends FromString[String] {
        override def fromString(string: String): String =   string
    }
}

case class Element(x:String){
    override def toString = s"$x"
    def as[T](implicit cast: FromString[T]): Option[T] = Try(cast.fromString(this.x)).toOption
}

case class Line(_1:String, _2:String, rest:List[Element], columnName:Option[List[String]]=None){
    final val offset = 3
    def index(n:String):Option[Int] = columnName.map(lst => lst.indexOf(n))
    //2017-05-04
    def date = LocalDate.parse(this._1, DateTimeFormatter.ISO_LOCAL_DATE)
    //12:18:02
    def time = LocalTime.parse(this._2, DateTimeFormatter.ISO_LOCAL_TIME)
    //Access given a column number
    def apply[T](n:Int)(implicit cast: FromString[T]):Option[T] = rest((n-this.offset).abs).as[T](cast)
    //access given a Column name, All columns names till required columns should be mentioned
    //this is not tested....., test this
    def apply[T](n:String)(implicit cast: FromString[T]):Option[T] = index(n).flatMap{(e:Int) => apply[T](e+this.offset)(cast)}
    //derived field
    def year = this.date.getYear
    //...
    def hour = this.time.getHour
 }
 
 object MyRdd extends App {
    val file = "/mapr/bht01/projects/wpscor/data/pkanshu/sampleFQStat"
    val firstRdd = sc.textFile(file)
    val firstline = firstRdd.first()
    val secondRdd = firstRdd.filter(row => row != firstline)
    val trdRdd = secondRdd.map( e => e.split(raw"\^"))
    val t2 = trdRdd.map{case Array(x,y,z @ _*) => (x,y,z.map(e=>Element(e)).toList)}
    t2.first()
 }
