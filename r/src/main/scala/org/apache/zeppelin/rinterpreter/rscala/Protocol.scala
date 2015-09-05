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

//Originally by David Dahl and released under the BSD license (used with permission).  See Package.scala for details

package org.apache.zeppelin.rinterpreter.rscala

// TODO implement 5-6
// TODO implement 16-19

object Protocol {

  // Data Types
  val UNSUPPORTED_TYPE = 0
  val INTEGER = 1
  val DOUBLE =  2
  val BOOLEAN = 3
  val STRING =  4
  val DATE = 5
  val DATETIME = 6

  // Data Structures
  val UNSUPPORTED_STRUCTURE = 10
  val NULLTYPE  = 11
  val REFERENCE = 12
  val ATOMIC    = 13
  val VECTOR    = 14
  val MATRIX    = 15
  val LIST      = 16
  val DATAFRAME = 17
  val S3CLASS   = 18
  val S4CLASS   = 19
  val JOBJ      = 20

  // Commands
  val EXIT          = 100
  val RESET         = 101
  val GC            = 102
  val DEBUG         = 103
  val EVAL          = 104
  val SET           = 105
  val SET_SINGLE    = 106
  val SET_DOUBLE    = 107
  val GET           = 108
  val GET_REFERENCE = 109
  val DEF           = 110
  val INVOKE        = 111
  val SCALAP        = 112

  // Result
  val OK = 1000
  val ERROR = 1001
  val UNDEFINED_IDENTIFIER = 1002

  // Misc.
  val CURRENT_SUPPORTED_SCALA_VERSION = "2.10"

}

