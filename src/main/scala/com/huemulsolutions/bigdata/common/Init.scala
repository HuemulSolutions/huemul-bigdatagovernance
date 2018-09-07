package com.huemulsolutions.bigdata.common

import com.huemulsolutions.bigdata.control.huemul_Control
import com.huemulsolutions.bigdata.tables.huemulType_Frecuency

object Init {
  def main(args: Array[String]): Unit = {
    val Global: huemul_GlobalPath = new huemul_GlobalPath()
    Global.GlobalEnvironments = "production,experimental"
 
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"BigData API", args, Global)
    
    val Control = new huemul_Control(huemulBigDataGov,null, huemulType_Frecuency.ANY_MOMENT, false, false)
    //Control.Init_CreateTables()
  }
}