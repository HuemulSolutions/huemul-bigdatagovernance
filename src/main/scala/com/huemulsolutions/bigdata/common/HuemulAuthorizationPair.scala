package com.huemulsolutions.bigdata.common

class HuemulAuthorizationPair(className: String, packageName: String) extends Serializable {
  def getLocalClassName: String = { className.replace("$", "")}
  def getLocalPackageName: String = { packageName.replace("$", "")}
}
