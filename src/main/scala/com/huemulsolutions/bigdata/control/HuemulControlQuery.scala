package com.huemulsolutions.bigdata.control

class HuemulControlQuery extends Serializable  {
  var queryId: String = _
  var tableAliasName: String = _
  var tableName: String = _
  var rawFilesDetId: String = _
  var isRaw: Boolean = false
  var isTable: Boolean = false
  var isTemp: Boolean = false
}
