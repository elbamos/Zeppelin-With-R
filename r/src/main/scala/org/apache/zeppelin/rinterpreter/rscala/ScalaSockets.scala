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
import java.net._

import org.slf4j.{Logger, LoggerFactory}

private[rinterpreter] class ScalaSockets(portsFilename: String) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val serverIn  = new ServerSocket(0,0,InetAddress.getByName(null))
  val serverOut = new ServerSocket(0,0,InetAddress.getByName(null))

  locally {
    logger.info("Trying to open ports filename: "+portsFilename)
    val portNumberFile = new File(portsFilename)
    val p = new PrintWriter(portNumberFile)
    p.println(serverIn.getLocalPort+" "+serverOut.getLocalPort)
    p.close()
    logger.info("Servers are running on port "+serverIn.getLocalPort+" "+serverOut.getLocalPort)
  }

  val socketIn = serverIn.accept
  logger.info("serverinaccept done")
  val in = new DataInputStream(new BufferedInputStream(socketIn.getInputStream))
  logger.info("in has been created")
  val socketOut = serverOut.accept
  logger.info("serverouacceptdone")
  val out = new DataOutputStream(new BufferedOutputStream(socketOut.getOutputStream))
  logger.info("out is done")
}

