package com.cathay.dtag.spark.rdd


// Simple class
class Person(var firstName: String, var lastName: String) {

  println("the constructor begins")

  // some class fields
  var age = 0
  private val userID = "A123456789"

  // some methods
  override def toString = s"$firstName $lastName is $age years old"
  def printID { println(s"my id = $userID") }
  def printFullName { println(this) }  // uses toString

  println("still in the constructor")

}

// Case class (only for Scala)
case class Company (name: String)

// Object (only one)
object DTAG {
  val employeeCount = 40
  val divisions = Array("CM", "BA", "DS")

  def introduce: Unit = {
    println("employee count: " + employeeCount)
    println("divisions:")
    divisions.foreach(println)
  }
}

object RunPerson extends App {
  // class example
  println("==class example==")
  val adam = new Person("Adam", "Meyer")
  println("adam firstName: " + adam.firstName)
  println("adam lastName: " + adam.lastName)

  val roger = new Person("Roger", "Kuo")
  roger.printID
  roger.printFullName
  roger.age = 27
  println("Roger age: " + roger.age)

  // case class example
  println("==case class example==")
  val c = Company("Cathay")
  println("Company name" + c.name)

  c match {
    case Company("Cathay") => println("Good")
    case Company("Fubon") => println("kerker")
    case _ => println("what ever")
  }

  // object example
  println("==object example==")
  DTAG.introduce
}
