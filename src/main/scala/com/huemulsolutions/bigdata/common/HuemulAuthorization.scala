package com.huemulsolutions.bigdata.common

import scala.collection.mutable._


class HuemulAuthorization extends Serializable  {
  private val access: ArrayBuffer[HuemulAuthorizationPair] = new ArrayBuffer[HuemulAuthorizationPair]()
  
  def addAccess(className: String, packageName: String) {
    access.append(new HuemulAuthorizationPair(className.replace("$", ""), packageName.replace("$", "")))
  }
  
  def hasAccess(className: String, packageName: String): Boolean = {
    //Access.foreach { x => println(s"""${x.getLocalClassName().toUpperCase()} == ${ClassName.replace("$", "").toUpperCase()} && ${x.getLocalPackageName().toUpperCase()} == ${PackageName.replace("$", "").toUpperCase()}""" ) }
    val values = access.filter { x => x.getLocalClassName.toUpperCase() == className.replace("$", "").toUpperCase() && x.getLocalPackageName.toUpperCase() == packageName.replace("$", "").toUpperCase()  }
    
    values.nonEmpty || access.isEmpty
  }
}