package com.huemulsolutions.bigdata.tables

object huemulType_Tables extends Enumeration {
  type huemulType_Tables = Value
  val Reference, Master, Transaction = Value
}


object huemulType_InternalTableType extends Enumeration {
  type huemulType_InternalTableType = Value
  val Normal, DQ, OldValueTrace = Value
}
