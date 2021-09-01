package com.huemulsolutions.bigdata.common

import scala.collection.mutable._


class HuemulAuthorization extends Serializable  {
  private val Access: ArrayBuffer[HuemulAuthorizationPair] = new ArrayBuffer[HuemulAuthorizationPair]()
  
  def AddAccess(ClassName: String, PackageName: String) {
    Access.append(new HuemulAuthorizationPair(ClassName.replace("$", ""), PackageName.replace("$", "")))
  }
  
  def HasAccess(ClassName: String, PackageName: String): Boolean = {
    //Access.foreach { x => println(s"""${x.getLocalClassName().toUpperCase()} == ${ClassName.replace("$", "").toUpperCase()} && ${x.getLocalPackageName().toUpperCase()} == ${PackageName.replace("$", "").toUpperCase()}""" ) }
    val values = Access.filter { x => x.getLocalClassName.toUpperCase() == ClassName.replace("$", "").toUpperCase() && x.getLocalPackageName.toUpperCase() == PackageName.replace("$", "").toUpperCase()  }
    
    values.nonEmpty || Access.isEmpty
  }
}