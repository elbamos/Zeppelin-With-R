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

import org.apache.spark.SparkContext
import org.apache.zeppelin.{RTest, SparkTest}
import org.apache.zeppelin.interpreter.InterpreterGroup
import org.apache.zeppelin.rinterpreter.rscala.RException
import org.apache.zeppelin.spark.SparkInterpreter
import org.scalatest.Matchers._
import org.scalatest._

import scala.concurrent._
import scala.concurrent.duration._


/**
 * Created by aelberg on 8/23/15.
 */
class RContextSparkTest extends FlatSpec {
  RContext.resetRcon()

    val props = new Properties()

    val sparky = new SparkInterpreter(props)
    val group = new InterpreterGroup



  "The spark interpreter" should "exist" in {
    sparky shouldBe a [SparkInterpreter]
  }


  it should "accept being in a group" in {
    group.add(sparky)
    sparky.setInterpreterGroup(group)
    assert(true)
  }


  it should "not be initialized yet" in {
    assertResult(false) {sparky.isSparkContextInitialized}
  }
  it should "open" taggedAs(SparkTest) in {
    sparky.open()
    assert(true)
  }

  it should "be initialized now" taggedAs(SparkTest) in {
    assertResult(true) {sparky.isSparkContextInitialized}
  }
  it should "be able to give us the spark context"  taggedAs(SparkTest) in {
    val sc = sparky.getSparkContext
    sc shouldBe a [SparkContext]
  }

  val rcon = RContext(props)
  "An RContext with Spark" should "exist" in {
    rcon shouldBe a [RContext]
  }
  it should "never have been started" in {
    rcon.sparkRStarted shouldBe false
  }

  it should "take the Group without complaining" in {
    rcon.setInterpreterGroup(group)
    assert(true)
  }

  it should "still be closed" in {
    rcon.isOpen shouldBe false
  }

  it should "be openable"  taggedAs(RTest, SparkTest) in {
    rcon.open(true)
    assert(rcon.isOpen)
  }

  it should "not throw an exception waiting to start" taggedAs(SparkTest) in {
    assume(rcon.isOpen)
    rcon.waitOnSpark()
    assert(true)
  }
  it should "have a spark thread" taggedAs(SparkTest) in {
    assume(rcon.isOpen)
    rcon.sparkStartupFuture shouldBe a [Some[_]]
  }
  it should "wait for the spark thread to finish" taggedAs(SparkTest) in {
    Await.ready(rcon.sparkStartupFuture.get, 2 minutes)
    assertResult(true) {rcon.sparkStartupFuture.get.isCompleted}
  }

  it should "have found SparkR" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assertResult(true) {rcon.testRPackage("SparkR")}
  }

  it should "have a Spark Backend with a non-zero port, which means it initialized" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assert(rcon.backend.port != 0)
  }
  it should "have a living Spark Backend thread" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assume(rcon.backend.port != 0)
    assert(rcon.backend.backendThread.isAlive)
  }

  it should "now agree that SparkR is open" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    assert(rcon.sparkRStarted)
  }

  "The RContext and SparkInterpreter Spark states" should "match" taggedAs(RTest, SparkTest) in {
    assert(sparky.isSparkContextInitialized == rcon.sparkRStarted)
  }

  "The RContext" should "be able to confirm that stats is available" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assertResult(true) {rcon.testRPackage("stats")}
  }

  it should "be able to confirm that rzeppelin is available" taggedAs(RTest) in {
    assume(rcon.isOpen)
    assertResult(true) {rcon.testRPackage("rzeppelin")}
  }
  it should "be able to confirm that a bogus package is not available" in {
    assume(rcon.isOpen)
    assertResult(false) {rcon.testRPackage("thisisagarbagepackagename")}
  }
  it should "be able to add 2 + 2" in {
    assume(rcon.isOpen)
    assertResult(4) {rcon.evalI0("2 + 2")}
  }
  it should "throw an RException if told to evaluate garbage code" in {
    assume(rcon.isOpen)
    intercept[RException] {
      rcon.eval("funkyfunction()")
    }
  }

  it should "verify that the spark start time has been set" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    val proof = rcon.evalS1("names(SparkR:::.sparkREnv)")
    proof should contain (".scStartTime")
  }

  it should "verify that the spark context exists" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    val proof = rcon.evalS1("names(SparkR:::.sparkREnv)")
    proof should contain (".sparkRjsc")
  }

  it should "verify that the sql context exists" taggedAs(RTest, SparkTest) in {
    assume(rcon.isOpen)
    val proof = rcon.evalS1("names(SparkR:::.sparkREnv)")
    proof should (contain (".sparkRSQLc") or contain (".sparkRHivesc"))
  }

  it should "also close sparky politely" taggedAs(SparkTest) in {
    sparky.close()
    assert(true)
  }

  it should "also close the context politely" in {
    assume(rcon.isOpen)
    rcon.close()
    assert(!rcon.isOpen)
  }

}
