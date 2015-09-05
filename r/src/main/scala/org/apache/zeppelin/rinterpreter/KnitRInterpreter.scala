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


// TODO:  Capture the knitr progress bar

import java.util._

import org.apache.zeppelin.interpreter._
import org.apache.zeppelin.rinterpreter.rscala.RException

class KnitRInterpreter(property: Properties, startSpark : Boolean = true) extends RInterpreter(property, startSpark) {
  def this(property : Properties) = {
    this(property, true)
  }


//  synchronized {
//    getInterpreterGroup.collectFirst({
//      case x : Interpreter if x.getClassName == "org.apache.zeppelin.rinterpreter.RRepl" => findrContextRecurse(x)
//      case x : Interpreter if x.getClassName == "org.apache.zeppelin.rinterpreter.RReplInterpreter" => findrContextRecurse(x)
//    })
//  }.get
////    getInterpreterGroup.find((x: Interpreter) => x.getClassName match {
////      case "org.apache.zeppelin.rinterpreter.RRepl" => true
////      case "org.apache.zeppelin.rinterpreter.RReplInterpreter" => true
////      case _ => false
////    })
////
////  }
//
//  private def findrContextRecurse(x : Interpreter) : RContext = {
//    x match {
//      case y : RReplInterpreter => y.getrContext
//      case y : RRepl => findrContextRecurse(y.getInnerInterpreter)
//      case y : LazyOpenInterpreter => findrContextRecurse(y.getInnerInterpreter)
//      case y : WrappedInterpreter => findrContextRecurse(y.getInnerInterpreter)
//      case y => {
//        val msg = "Found instead of an RReplInterpreter an " + y.toString()
//        logger.error(msg)
//        throw new RuntimeException(msg)
//      }
//    }
//  }

  override def open: Unit = {
    logger.info("Opening knitr")
    rContext.synchronized {
      rContext.setInterpreterGroup(getInterpreterGroup)
      super.open
      logger.debug("Knitr open, initial commands")
      rContext.testRPackage("knitr", true, true, "Without knitr, the knitr interpreter cannot run.")
      rContext.eval(
        """opts_knit$set(out.format = 'html',
          |results='asis',
          |progress = FALSE,
          |self.contained = TRUE,
          |verbose = FALSE,
          |comment = NA,
          |echo = FALSE,
          |tidy = FALSE)
          | """.stripMargin)
    }
    logger.info("KnitR:  Finished initial commands")
  }

  def interpret(st: String, context: InterpreterContext): InterpreterResult = try {
    rContext.waitOnSpark()
    logger.debug("interpreting" + st)
    // need to convert st into an array of Strings within R
    val out: String = rContext.synchronized {
      rContext.set(".zeppknitrinput", st.split("\n"))
      rContext.eval(".knitout <- knit2html(text=.zeppknitrinput, envir = rzeppelin:::.zeppenv)")
      rContext.getS0(".knitout")
    }
    logger.debug("Assemble  output")

    new InterpreterResult(InterpreterResult.Code.SUCCESS,
      InterpreterResult.Type.HTML,
      RInterpreter.processHTML(out)
    )
  } catch {
    case r: RException => r.getInterpreterResult(st)
    case e: Exception => new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage())
  }
}

