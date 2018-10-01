package com.huemulsolutions.bigdata.common

class huemul_LibraryParams() extends Serializable {
  var param_name: String = null
  var param_value: String = null
  var param_type: String = null //function, program
}

class huemul_DateTimePart(MiliSeconds: Long) extends Serializable {
  private var calc = MiliSeconds / 1000
  val second = calc % 60
  calc /= 60
  val minute = calc % 60
  calc /= 60
  val hour = calc % 24
  calc /= 24
  val days = calc
    
}