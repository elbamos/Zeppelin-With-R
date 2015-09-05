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

import org.apache.zeppelin.RTest
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}
import org.scalatest.Matchers._
import org.scalatest._

/**
 * Created by aelberg on 8/17/15.
 */
class RInterpreterTest extends FlatSpec {
  "props" should "include spark.home" in {() =>
    assert(RInterpreter.props.get("spark.home") != null)
  }

  RContext.resetRcon()

  class RIntTester extends RInterpreter(new Properties(), startSpark = false) {

    def interpret(s: String, interpreterContext: InterpreterContext): InterpreterResult = {
      val result : Array[String] = rContext.evalS1(s)
      new InterpreterResult(InterpreterResult.Code.SUCCESS, result.mkString("\n"))
    }
  }
  val rint = new RIntTester()

  "An RInterpreter" should "exist" in {
    assert(rint != null)
  }

  it should "have a fresh rContext" in {
    assert(! rint.getrContext.isOpen)
  }

  it should "open"  taggedAs(RTest) in {
    rint.open()
    assert(rint.getrContext.isOpen)
  }

  it should "have rzeppelin available"  taggedAs(RTest) in {
    assume(rint.getrContext.isOpen)
    assert(rint.getrContext.testRPackage("rzeppelin"))
  }
  it should "have an rContext able to do simple addition" taggedAs(RTest)  in {
    assume(rint.getrContext.isOpen)
    assert(rint.getrContext.evalI0("2 + 2") == 4)
  }



  it should "have a functional completion function" taggedAs(RTest) in {
    val result = rint.hiddenCompletion("hi", 3)
    result should (contain ("hist"))
  }

  it should "have a working progress meter" in {
    rint.getrContext.setProgress(50)
    assertResult(50) {
      rint.getrContext.getProgress
    }
  }

  it should "have persistent properties" in {
    val props = new Properties()
    props.setProperty("hello", "world")
    rint.setProperty(props)
    assertResult("world") {
      rint.getProperty("hello")
    }
  }

  var rint2 : RIntTester = null

  it should "Share RContexts" in {
    rint2 = new RIntTester()
    rint2.open()
    rint.getrContext should be theSameInstanceAs rint2.getrContext
  }

  "Opening the second RInterpreter" should "not have closed the first RContext" in {
    assert(rint.getrContext.isOpen)
  }

  "The first RInterpreter" should "close politely" in {
    rint.close()
    assert(!rint.getrContext.isOpen)
  }

  "and so" should "the other one" in {
    rint2.close()
    assert(!rint2.getrContext.isOpen)
  }

//  fixture.sparky.close()

}
