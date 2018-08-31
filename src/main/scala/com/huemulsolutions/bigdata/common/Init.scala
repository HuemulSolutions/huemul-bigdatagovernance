package com.huemulsolutions.bigdata.common

import com.huemulsolutions.bigdata.control.huemul_Control

object Init {
  def main(args: Array[String]): Unit = {
    val Global: huemul_GlobalPath = new huemul_GlobalPath()
    Global.GlobalEnvironments = "prod, desa, qa"
 
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"BigData API", args, Global)
    
    val Control = new huemul_Control(huemulBigDataGov,null, false, false)
    //Control.Init_CreateTables()
  }
}