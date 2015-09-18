/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.rinterpreter

import java.util.Properties

import org.apache.spark.api.java.JavaRDD
import org.apache.zeppelin.interpreter.{InterpreterGroup, InterpreterResult}
import org.apache.zeppelin.spark.SparkInterpreter
import org.apache.zeppelin.{RTest, SparkTest}
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.concurrent._
import scala.concurrent.duration._

/**
 * Created by aelberg on 8/24/15.
 */
class RSparkTest extends FlatSpec with GeneratorDrivenPropertyChecks {
  RContext.resetRcon()
  val props = new Properties()
  val sparky = new SparkInterpreter(props)
  val group = new InterpreterGroup
  group.add(sparky)
  sparky.setInterpreterGroup(group)

  "The Spark Interpreter" should "exist"  taggedAs(SparkTest) in {
    assert(sparky != null)
  }
  it should "not be initialized yet" taggedAs(SparkTest)in {
    assertResult(false) {sparky.isSparkContextInitialized}
  }
  it should "open without complaining" taggedAs(SparkTest)in {
    sparky.open()
    assert(true)
  }
  it should "be initialized" taggedAs(SparkTest)in {
    assertResult(true) {sparky.isSparkContextInitialized}
  }

  val rrepl = new RReplInterpreter(props, true)
  group.add(rrepl)
  rrepl.setInterpreterGroup(group)

  "The R REPL Interpreter with Spark" should "exist and be of the right class" in { 
    rrepl shouldBe a[RReplInterpreter]
  }

  val rcon : RContext = rrepl.getrContext

  "The RContext Inside" should "be fresh" in {
    assert(!rcon.isOpen)
  }

  it should "open without complaining" in {
    assume(sparky.isSparkContextInitialized)
    rcon.open(true)
    assertResult(true) { rcon.isOpen }
  }

  "An open RREplInterpreter with Spark" should "then open" taggedAs(RTest) in {
    assume(rcon.isOpen)
    rrepl.open()
    assert(true)
  }

  "Waiting for Spark to Startup" should "not throw an exception" taggedAs(SparkTest) in {
    assume(rcon.isOpen)
    rcon.waitOnSpark()
    assert(true)
  }

  "the RContext" should "have a sparkStartupFuture" in {
    assume(rcon.isOpen)
    rcon.sparkStartupFuture shouldBe a [Option[_]]
  }

  it should "patiently wait for the startupfuture" taggedAs(SparkTest)in {
    assume(rcon.isOpen)
    Await.ready(rcon.sparkStartupFuture.get, 2 minutes)
    assert(true)
  }

  it should "have completed the startupfuture" taggedAs(SparkTest)in {
    assume(rcon.isOpen)
    assertResult(true) {rcon.sparkStartupFuture.get.isCompleted}
  }

  it should "have completed the startupfuture successfully" taggedAs(SparkTest)in {
    assume(rcon.isOpen)
    Await.ready(rcon.sparkStartupFuture.get, 2 minutes)
    assertResult(true) {rcon.sparkRStarted}
  }

  it should "have started sparkr" taggedAs(RTest, SparkTest) in {
    assume(sparky.isSparkContextInitialized)
    assert(rcon.sparkRStarted)
  }

  "The R side of things" should "have evaluate available" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assert(rcon.testRPackage("evaluate"))
  }

  it should "have rzeppelin available" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assert(rcon.testRPackage("rzeppelin"))
  }

  it should "have SparkR available" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assert(rcon.testRPackage("SparkR"))
  }
  it should "be able to execute evaluate properly" taggedAs(RTest)  in {
    assume(rcon.isOpen)
    assume(rcon.testRPackage("evaluate"))
    val int: InterpreterResult = rrepl.interpret("names(.GlobalEnv)", null)
    int should have ('code (InterpreterResult.Code.SUCCESS))
  }

  it should "execute a simple command successfully" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assume(rcon.testRPackage("evaluate"))
    val int: InterpreterResult = rrepl.interpret("2 + 2", null)
    int should have ('code (InterpreterResult.Code.SUCCESS))
  }

  it should "produce an error if a command is garbage" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assume(rcon.testRPackage("evaluate"))
    val int: InterpreterResult = rrepl.interpret("2 %= henry", null)
    withClue(int.message()) {
      int should have('code (InterpreterResult.Code.ERROR))
    }
  }

  it should "return text for a good command" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assume(rcon.testRPackage("evaluate"))
    val int: InterpreterResult = rrepl.interpret("2 + 2", null)
    withClue(int.message()) {
      int should have('code (InterpreterResult.Code.SUCCESS),
        'type (InterpreterResult.Type.TEXT))
    }
  }

  it should "find the spark context" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    assume(rcon.sparkRStarted)
  val result = rrepl.interpret("str(sc)", null)
    withClue(result.message()) {
      result should have('code (InterpreterResult.Code.SUCCESS))
    }
  }
  it should "verify that the spark start time has been set" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    assume(rcon.sparkRStarted)
    val proof = rcon.evalS1("names(SparkR:::.sparkREnv)")
    withClue(proof) {
      proof should contain(".scStartTime")
    }
  }

  it should "verify that the spark context exists" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    assume(rcon.sparkRStarted)
    val proof = rcon.evalS1("names(SparkR:::.sparkREnv)")
    withClue(proof) {
      proof should contain(".sparkRjsc")
    }
  }

  it should "verify that the sql context exists" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    assume(rcon.sparkRStarted)
    val proof = rcon.evalS1("names(SparkR:::.sparkREnv)")
    withClue(proof) {
      proof should (contain(".sparkRSQLc") or contain(".sparkRHivesc"))
    }
  }
  it should "parallelize 1:10" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    assume(rcon.sparkRStarted)
    val result = rrepl.interpret("onetoten <- SparkR:::parallelize(sc, 1:10)", null)
    withClue(result.message) {
      result should have('code (InterpreterResult.Code.SUCCESS))
    }
  }

  it should "map an RDD" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    assume(rcon.sparkRStarted)
    val result = rrepl.interpret("mapped <- SparkR:::map(onetoten, function(x) x^2)", null)
    withClue(result.message) {
      result should have('code (InterpreterResult.Code.SUCCESS))
    }  }

  it should "reduce an RDD" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    assume(rcon.sparkRStarted)
    val result = rrepl.interpret("result <- SparkR:::reduce(mapped, function(x,y) x+y)", null)
    withClue(result.message) {
      result should have('code (InterpreterResult.Code.SUCCESS))
    }
  }
  it should "produce the right answer reducing an RDD" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    assume(rcon.sparkRStarted)
    val result = rrepl.interpret("SparkR:::reduce(mapped, function(x,y) x+y)", null)
    result.message() should include ("385")
  }
  it should "find a Spark SQL Context" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    assume(rcon.sparkRStarted)
    val result = rrepl.interpret("str(sqlContext)", null)
    withClue(result.message) {
      result should have('code (InterpreterResult.Code.SUCCESS))
    }
  }
  // TODO:  SQL & Hive

  //  it should "be able to find the table command" in {
  //    val rrepl = fixture.rrepl
  //    assertResult(false) {
  //      rcon.getB0("is.null(rzeppelin:::.z.table)")
  //    }
  //  }
  "The ZeppelinContext in the Spark Interpreter" should "not be null" taggedAs(SparkTest) in {
    assert(sparky.getZeppelinContext != null)
  }

  "RStatics" should "have a not-null Z" taggedAs(SparkTest) in {
    assert(RStatics.getZ != null)
  }

  "The ZeppelinContext" should "take from R a String placed through scala" taggedAs(SparkTest) in {
    assume(rcon.sparkRStarted)
    RStatics.getZ.put("integer", "hello")
    val result = rrepl.interpret("rzeppelin:::.z.get(\"integer\")", null)
    withClue("\n" + result.message() + "\n") {
      result.message should include ("hello")
    }
  }

  it should "take from scala a String placed through R" taggedAs(SparkTest) in {
    assume(rcon.sparkRStarted)
    val result = rrepl.interpret("rzeppelin:::.z.put(\"integer\", \"world\")", null)
    withClue(result.message()) {
      assertResult("world") {
        RStatics.getZ.get("integer")
      }
    }
  }

  it should "take from R an RDD placed through scala" taggedAs(SparkTest) in {
    assume(rcon.sparkRStarted)
    val jrdd: org.apache.spark.api.java.JavaRDD[Int] = JavaRDD.fromRDD(sparky.getSparkContext.parallelize(1 until 10))
    RStatics.getZ.put("rddtester", jrdd)
    val result = rrepl.interpret("rddtester <- rzeppelin:::.z.get(\"rddtester\")", null)
    withClue(result.message()) {
      result.code should be (InterpreterResult.Code.SUCCESS)
    }
  }
  it should "have an RDD in R of class RDD" taggedAs(SparkTest) in {
    val res2 = rrepl.interpret("class(rddtester)", null)
    withClue(res2.message()) {
      res2.message should include ("RDD")
    }
  }

  it should "be able to collect that RDD in R without error" taggedAs(SparkTest) in {
    val res2 = rrepl.interpret("SparkR:::collect(rddtester)", null)
    info(res2.message)
    res2.code should be (InterpreterResult.Code.SUCCESS)
  }

  it should "map the RDD into numbers" taggedAs(SparkTest) in {
    val result = rrepl.interpret("rdd2 <- SparkR:::map(rddtester, function(x) as.integer(x))", null)
    withClue(result.message()) {
      result.code shouldBe (InterpreterResult.Code.SUCCESS)
    }
  }

  it should "be able to reduce that RDD"  taggedAs(SparkTest) in {pending}
//    val result = rrepl.interpret("result <- SparkR:::reduce(rdd2, function(x, y) x + y)", null)
//    withClue(result.message()) {
//      result.code shouldBe (InterpreterResult.Code.SUCCESS)
//    }
//  }

  it should "get the right answer" taggedAs(SparkTest) in {pending}
//    val result = rrepl.interpret("result", null)
//    withClue(result.message()) {
//      result.message should include ("55")
//    }
//  }

  it should "take from scala an RDD placed through R"  taggedAs(SparkTest) in {
    val res1 = rrepl.interpret("othertester <- SparkR:::parallelize(sc, 1:10)", null)
    assume(res1.code() == InterpreterResult.Code.SUCCESS)
    val result = rrepl.interpret("rzeppelin:::.z.put(\"othertester\", othertester)", null)
    withClue(result.message()) {
      assume(result.code() == InterpreterResult.Code.SUCCESS)
    }
    withClue(res1.message() + result.message()) {
     RStatics.getZ.get("othertester") shouldBe a [JavaRDD[_]]
    }
  }


  it should "get the right result from that RDD" taggedAs(SparkTest) in {pending}
//    val theRDD: JavaRDD[Any] = RStatics.getZ.get("othertester").asInstanceOf[JavaRDD[Any]]
//    info(theRDD.collect().toString())
//    assert(false)
//  }

  "Progress" should "start at 0" in {
    assertResult(0) {
      rrepl.getProgress(null)
    }
  }
  it should "be settable to 50" in {
    rrepl.getrContext.setProgress(50)
    assertResult(50) {
      rrepl.getProgress(null)
    }
  }
  it should "incremenet to 51" in {
    rrepl.getrContext.incrementProgress(1)
    assertResult(51) {
      rrepl.getProgress(null)
    }
  }
  workers(1)
  "The progress function" should "allow set progress mod 100" in {pending}
//    forAll { (n: Int) => whenever (n >= 0 && n <= 100) {
//        val result = rrepl.interpret(s"rzeppelin:::.z.setProgress($n)", null)
//        withClue(result.message()) {
//          assertResult(n % 100) {
//            rrepl.getProgress(null)
//          }
//        }
//      }
//    }
//  }

  it should "accept set progress increments" in {pending}
//    forAll { (n: Int) => whenever(n >= -100 && n <= 100) {
//      val old = rrepl.getProgress(null)
//      val result = rrepl.interpret(s"rzeppelin:::.z.incrementProgress($n)", null)
//      withClue(result.message()) {
//        assertResult(old + n % 100) {
//          rrepl.getProgress(null)
//        }
//      }
//    }
//    }
//  }

  it should "set progress correctly when told to do so arbitrarily" in {pending}
//    forAll { (n: Int) => whenever(n >= -100 & n <= 100) {
//      val result = rrepl.interpret(s"rzeppelin:::.z.setProgres($n)", null)
//      val result2 = rrepl.interpret("rzeppelin:::.z.incrementProgress()", null)
//      withClue(result.message() + result2.message()) {
//        assertResult((n % 100) + 1 % 100) {
//          rrepl.getProgress(null)
//        }
//      }
//    }
//    }
//  }

  "the RREPL Interpreter" should "produce an error if a command is garbage"  taggedAs(RTest) in {
    assume(rcon.isOpen)
    val intr : InterpreterResult = rrepl.interpret("2 %= henry", null)
    withClue(intr.message) {
      intr should have ('code (InterpreterResult.Code.ERROR),
        'type (InterpreterResult.Type.TEXT))
    }
  }
  it should "execute a simple command successfully"  taggedAs(RTest) in {
    assume(rcon.isOpen)
    val intr : InterpreterResult = rrepl.interpret("2 + 2", null)
    withClue(intr.message) {
      intr should have('code (InterpreterResult.Code.SUCCESS),
        'type (InterpreterResult.Type.TEXT))
    }
  }

  it should "handle a plot"  taggedAs(RTest) in {
    assume(rcon.isOpen)
    val intr : InterpreterResult = rrepl.interpret("hist(rnorm(100))", null)
    withClue(intr.message) {
      intr should have('type (InterpreterResult.Type.IMG), 'code (InterpreterResult.Code.SUCCESS))
    }
  }
  //TODO:  Test the HTML parser
  it should "handle a data.frame"  taggedAs(RTest) in {
    assume(rcon.isOpen)
    assume(rcon.testRPackage("evaluate"))
    val intr : InterpreterResult = rrepl.interpret("data.frame(coming = rnorm(100), going = rnorm(100))", null)
    withClue(intr.message) {
      intr should have('type (InterpreterResult.Type.TABLE), 'code (InterpreterResult.Code.SUCCESS))
    }
  }

  // Not testing because requires repr and base64enc

  //  it should "handle an image" in {
  //    val repl = fixture.repl
  //    val int : InterpreterResult = repl.interpret("hist(rnorm(100))", null)
  //    assert(int.`type`() == InterpreterResult.Type.IMG)
  //  }



  "The RContext" should "close politely" taggedAs(SparkTest)  in {
    assume(rcon.isOpen)
    rcon.backend.close()
    rcon.backend.backendThread.stop()
    rrepl.close()
    rcon.close()
    rcon.backend.close()
    assertResult(false) {
      rcon.isOpen
    }
  }

  "so" should "the spark context" taggedAs(SparkTest) in {
    assume(sparky.isSparkContextInitialized)
    sparky.close()
    assert(true)
  }

}