package com.huemulsolutions.bigdata.tables

import com.huemulsolutions.bigdata.dataquality.HuemulTypeDQNotification.HuemulTypeDQNotification

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.dataquality._



class HuemulTableRelationship(Class_TableName: Object, allowNull: Boolean) {
  val Relationship: ArrayBuffer[HuemulTableRelationshipColumns] = new ArrayBuffer[HuemulTableRelationshipColumns]()
  var MyName: String = _
  val AllowNull: Boolean = allowNull
  def AddRelationship (PK: HuemulColumns, FK: HuemulColumns) {
      Relationship.append(new HuemulTableRelationshipColumns(PK, FK) )
  }
  val _Class_TableName: Object = Class_TableName

  private var _Notification: com.huemulsolutions.bigdata.dataquality.HuemulTypeDQNotification.HuemulTypeDQNotification = HuemulTypeDQNotification.ERROR
  def setNotification(value: HuemulTypeDQNotification ): HuemulTableRelationship = {
    _Notification = value
    this
  }
  def getNotification: HuemulTypeDQNotification =  _Notification

  //From 2.1 --> apply broadcast
  private var _BroadcastJoin: Boolean = false
  def broadcastJoin(value: Boolean ): HuemulTableRelationship = {
    _BroadcastJoin = value
    this
  }
  def getBroadcastJoin: Boolean =  _BroadcastJoin

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