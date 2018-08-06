package com.huemul.bigdata.common

import com.huemul.bigdata.control.huemul_Control

object Init {
  def main(args: Array[String]): Unit = {
    val Global: huemul_GlobalPath = new huemul_GlobalPath()
    Global.GlobalEnvironments = "prod, desa, qa"
 
    val huemulLib  = new huemul_Library(s"BigData API", args, Global)
    
    val Control = new huemul_Control(huemulLib,null, false, false)
    Control.Init_CreateTables()
  }
}