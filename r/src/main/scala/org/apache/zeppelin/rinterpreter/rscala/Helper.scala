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

import java.io._

object Helper {

  def writeString(out: DataOutputStream, string: String): Unit = {
    val bytes = string.getBytes("UTF-8")
    val length = bytes.length
    out.writeInt(length)
    out.write(bytes,0,length)
  }

  def readString(in: DataInputStream): String = {
    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    new String(bytes,"UTF-8")
  }

  def isMatrix[T](x: Array[Array[T]]): Boolean = {
    if ( x.length != 0 ) {
      val len = x(0).length
      for ( i <- 1 until x.length ) {
        if ( x(i).length != len ) return false
      }
    }
    true
  }

  def main(args: Array[String]): Unit = {
    print(util.Properties.versionNumberString)
  }

}

