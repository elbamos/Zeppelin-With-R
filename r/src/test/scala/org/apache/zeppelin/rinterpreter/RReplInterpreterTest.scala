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
import org.apache.zeppelin.interpreter.InterpreterResult
import org.scalatest.Matchers._
import org.scalatest._

/**
 * Created by aelberg on 8/17/15.
 */
class RReplInterpreterTest extends FlatSpec {
  RContext.resetRcon()

  val repl : RInterpreter = new RReplInterpreter(new Properties(), startSpark = false)

  "The R REPL Interpreter" should "exist and be of the right class" in {

    repl shouldBe a [RReplInterpreter]
  }

  it should "have a fresh rContext" in {

    assert(! repl.getrContext.isOpen)
  }

  it should "open"  taggedAs(RTest) in {

    repl.open()
    assert(repl.getrContext.isOpen)
  }

  val rcon : RContext = repl.getrContext
  it should "have evaluate available"  taggedAs(RTest) in {
    assume(rcon.isOpen)
    assert(rcon.testRPackage("evaluate"))
  }

  it should "actually not produce an error on the first evaluate, but we can fix that later" in {
    assume(rcon.isOpen)
    val intr : InterpreterResult = repl.interpret("2 + 2", null)
    withClue(intr.toString) {
      intr should have('code (InterpreterResult.Code.ERROR),
        'type (InterpreterResult.Type.TEXT))
    }
  }

  it should "execute a simple command successfully"  taggedAs(RTest) in {
    assume(rcon.isOpen)
    val intr : InterpreterResult = repl.interpret("2 + 2", null)
    withClue(intr.toString) {
      intr should have('code (InterpreterResult.Code.SUCCESS),
      'type (InterpreterResult.Type.TEXT))
    }
  }

  it should "produce an error if a command is garbage"  taggedAs(RTest) in {
    assume(rcon.isOpen)
    val intr : InterpreterResult = repl.interpret("2 %= henry", null)
    withClue(intr.message) {
    intr should have ('code (InterpreterResult.Code.ERROR),
      'type (InterpreterResult.Type.TEXT))
    }
  }

//  it should "be able to find the table command" in {
//    val repl = fixture.repl
//    assertResult(false) {
//      repl.getrContext.getB0("is.null(rzeppelin:::.z.table)")
//    }
//  }
  it should "handle a plot"  taggedAs(RTest) in {
    assume(rcon.isOpen)
    val intr : InterpreterResult = repl.interpret("hist(rnorm(100))", null)
    withClue(intr.message) {
      intr should have('type (InterpreterResult.Type.IMG), 'code (InterpreterResult.Code.SUCCESS))
    }
  }
  //TODO:  Test the HTML parser
  it should "handle a data.frame"  taggedAs(RTest) in {
    assume(rcon.isOpen)
    assume(rcon.testRPackage("evaluate"))
    val intr : InterpreterResult = repl.interpret("data.frame(coming = rnorm(100), going = rnorm(100))", null)
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

  // Handle HTML here

  it should "also close politely" in {
    assume(rcon.isOpen)
    repl.close()
    assertResult(false) {rcon.isOpen}
  }

}
