package com.cathay.dtag.spark.rdd

/**
  * Created by roger19890107 on 7/27/16.
  */
class MyClass(init: Int) {
  def subtract(x: Int, y: Int) = x - y + init
}

object MyObj {
  def multiply(x: Int, y: Int) = x * y
}

object RunFunction extends App {
  // basic function with function as parameter
  def myFunction(a: Int, b: Int, f: (Int, Int) => Int): Unit = {
    val result = f(a, b)
    println("result: " + result)
  }

  // specific function as parameter
  def sum(a: Int, b: Int) = a + b
  myFunction(1, 2, sum)

  // pass anonymous function
  myFunction(1, 2, (x, y) => x + y)

  // pass super concise function
  myFunction(1, 2, _ + _)

  // split 2 parameter list
  def myFunction2(a: Int, b: Int)(f: (Int, Int) => Int): Unit = {
    val result = f(a, b)
    println("result: " + result)
  }

  myFunction2(1, 2)(sum)

  val funWith_1_2 = myFunction2(1, 2)_
  funWith_1_2((x, y) => x + y)

  // instance or object's methods as parameter
  val myClass = new MyClass(5)
  myFunction(3, 4, myClass.subtract)

  myFunction(5, 5, MyObj.multiply)
}
