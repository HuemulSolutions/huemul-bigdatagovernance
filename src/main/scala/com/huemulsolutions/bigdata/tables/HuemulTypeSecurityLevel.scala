package com.huemulsolutions.bigdata.tables

object HuemulTypeSecurityLevel extends Enumeration {
  type HuemulTypeSecurityLevel = Value
  val Secret, Restricted, Confidential, Intern, Departamental, Public = Value
}

