package com.huemulsolutions.bigdata.common

class huemul_AuthorizationPair(ClassName: String,  PackageName: String) extends Serializable {
  def getLocalClassName: String = { ClassName.replace("$", "")}
  def getLocalPackageName: String = { PackageName.replace("$", "")}
}
