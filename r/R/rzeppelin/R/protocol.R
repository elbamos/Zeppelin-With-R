#Copyright (c) 2013-2015, David B. Dahl, Brigham Young University
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are
#met:
#
#Redistributions of source code must retain the above copyright
#notice, this list of conditions and the following disclaimer.
#
#Redistributions in binary form must reproduce the above copyright
#notice, this list of conditions and the following disclaimer in
#the documentation and/or other materials provided with the
#distribution.
#
#Neither the name of the <ORGANIZATION> nor the names of its
#contributors may be used to endorse or promote products derived
#from this software without specific prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
#"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
#LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
#A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
#HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
#THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

UNSUPPORTED_TYPE <- 0L
INTEGER <- 1L
DOUBLE <-  2L
BOOLEAN <- 3L
STRING <-  4L
DATE <- 5L
DATETIME <- 6L
UNSUPPORTED_STRUCTURE <- 10L
NULLTYPE  <- 11L
REFERENCE <- 12L
ATOMIC    <- 13L
VECTOR    <- 14L
MATRIX    <- 15L
LIST <- 16L
DATAFRAME <- 17L
S3CLASS <- 18L
S4CLASS <- 19L
JOBJ <- 20L
EXIT          <- 100L
RESET         <- 101L
GC            <- 102L
DEBUG         <- 103L
EVAL          <- 104L
SET           <- 105L
SET_SINGLE    <- 106L
SET_DOUBLE    <- 107L
GET           <- 108L
GET_REFERENCE <- 109L
DEF           <- 110L
INVOKE        <- 111L
SCALAP        <- 112L
OK <- 1000L
ERROR <- 1001L
UNDEFINED_IDENTIFIER <- 1002L
CURRENT_SUPPORTED_SCALA_VERSION <- "2.10"
