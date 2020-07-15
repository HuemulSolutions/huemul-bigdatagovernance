package com.huemulsolutions.bigdata.common

class huemul_KeyValuePath(Environment: String, PathOrDataBase: String) extends Serializable {
  /**example: "prod, desa, qa"
   **/
  val environment: String = Environment
  /** Value: Path for Files, DataBase Name for hive tables
   */
  val Value: String = PathOrDataBase
}
