package com.huemulsolutions.bigdata.control

class HuemulControlQuery extends Serializable  {
  var query_id: String = _
  var tableAlias_name: String = _
  var table_name: String = _
  var rawFilesDet_Id: String = _
  var isRAW: Boolean = false
  var isTable: Boolean = false
  var isTemp: Boolean = false
}
