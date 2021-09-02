package com.huemulsolutions.bigdata.common

class HuemulKeyValuePath(environment: String, pathOrDataBase: String) extends Serializable {
  /**example: "prod, desa, qa"
   **/
  //val environment: String = environment
  /** Value: Path for Files, DataBase Name for hive tables
   */
  val value: String = pathOrDataBase
  def getEnvironment: String = environment;


  //from 2.6.1 add user and password, issue #111
  private var userName: String = _
  /**
   * set userName to connect DB
   * @param user userName
   * @return huemul_KeyValuePath
   */
  def setUserName(user: String): HuemulKeyValuePath = {
    userName = user
    this
  }


  /**
   * get userName to connect to DB
   * @return
   */
  def getUserName: String = userName

  private var password: String = _
  /**
   * set password
   * @param password password
   * @return huemul_KeyValuePath
   */
  def setPassword(password: String): HuemulKeyValuePath = {
    this.password = password
    this
  }
  def getPassword: String = password
}
