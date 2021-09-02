package com.huemulsolutions.bigdata.tables

import com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification.HuemulTypeDqNotification

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.dataquality._



class HuemulTableRelationship(classTableName: Object, allowNullParam: Boolean) {
  val relationship: ArrayBuffer[HuemulTableRelationshipColumns] = new ArrayBuffer[HuemulTableRelationshipColumns]()
  var myName: String = _
  val allowNull: Boolean = allowNullParam
  def addRelationship(PK: HuemulColumns, FK: HuemulColumns) {
      relationship.append(new HuemulTableRelationshipColumns(PK, FK) )
  }
  val _Class_TableName: Object = classTableName

  private var _notification: com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification.HuemulTypeDqNotification = HuemulTypeDqNotification.ERROR
  def setNotification(value: HuemulTypeDqNotification ): HuemulTableRelationship = {
    _notification = value
    this
  }
  def getNotification: HuemulTypeDqNotification =  _notification

  //From 2.1 --> apply broadcast
  private var _broadcastJoin: Boolean = false
  def broadcastJoin(value: Boolean ): HuemulTableRelationship = {
    _broadcastJoin = value
    this
  }
  def getBroadcastJoin: Boolean =  _broadcastJoin

  private var _externalCode: String = "HUEMUL_DQ_001"
  def getExternalCode: String = {
    //return
    if (_externalCode==null)
      "HUEMUL_DQ_001"
    else
      _externalCode
  }
  def setExternalCode(value: String): HuemulTableRelationship = {
    _externalCode = value  
    this
  }
  
}