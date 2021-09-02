package com.huemulsolutions.bigdata.dataquality

import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.tables.HuemulColumns
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDqQueryLevel._
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification._

/** huemul_DataQuality permite ejecutar validaciones en la misma tabla, ya sea
 *  a nivel de fila para un campo (campo_a > 10), comparar varios campos (campo_a > campo_b)
 *  o a nivel agrupado (sum(campo_a) > sum(campo_b)
 *  @constructor Inicializa variables en null
 *  @param fieldName nombre del campo de la validación, puede ser null si la validación es a nivel de tabla
 *  @param description descripción de la validación
 *  @param sqlFormula formula sql en positivo (ejemplo campo_a > campo_b para validar que campo_a debe ser mayor a campo_b)
 *  @param errorCode codigo de error, debe usar códigos que vayan entre 1 y 999
 *  @param queryLevel indica si es por fila (row) o agrupado (aggregate). por default es Row
 *  @param notification nivel de notificación, ERROR gatilla error y fuerza salida, WARNING es solo una alerta. por default es ERROR
 *  @param saveErrorDetails (default true) indica si guarda el detalle del error o warning de DQ en tabla de detalle
 *  @param dqExternalCode (Default null) indica el código externo de tabla de DQ
 */
class HuemulDataQuality(fieldName: HuemulColumns
                        , description: String
                        , sqlFormula: String
                        , errorCode: Integer
                        , queryLevel: HuemulTypeDqQueryLevel = HuemulTypeDqQueryLevel.Row //,IsAggregated: Boolean
                        , notification: HuemulTypeDqNotification = HuemulTypeDqNotification.ERROR //RaiseError: Boolean
                        , saveErrorDetails: Boolean = true
                        , dqExternalCode: String = null
            ) extends Serializable {

  private var _dqExternalCode: String = dqExternalCode
  private var toleranceErrorPercent: Decimal = _
  private var _queryLevel: HuemulTypeDqQueryLevel = queryLevel
  private var _notification: HuemulTypeDqNotification = notification
  private var _saveErrorDetails: Boolean = saveErrorDetails


  /**% of total for refuse validation. Example: 0.15 = 15% (null to not use)
   */
  def getToleranceErrorPercent: Decimal = toleranceErrorPercent

  private var toleranceErrorRows: java.lang.Long = 0
  /**N° of records for refuse validation. Example: 1000 = 1000 rows with error  (null to not use)
   */
  def getToleranceErrorRows: java.lang.Long = toleranceErrorRows

  /**SQL for validation, expressed in a positive way (boolean) . Example: Field1 < Field2 (field oK)
   */
  def getSqlFormula: String = sqlFormula //= null

  private var _Id: Integer = _
  def setId(Id: Integer) {_Id = Id}
  def getId: Integer = _Id




  def getFieldName: HuemulColumns = fieldName
  def getQueryLevel: HuemulTypeDqQueryLevel  = _queryLevel
  def getDescription: String  = description
  def getNotification: HuemulTypeDqNotification = _notification
  def getSaveErrorDetails: Boolean = {if (_queryLevel == HuemulTypeDqQueryLevel.Row) _saveErrorDetails else false}
  def getErrorCode: Integer = errorCode
  def setDqExternalCode(value: String) {_dqExternalCode = value }
  def getDqExternalCode: String =  _dqExternalCode
  var NumRowsOK: java.lang.Long = _
  var NumRowsTotal: java.lang.Long = _


  var ResultDQ: String = _

  /**
   * MyName
   */
  private var MyName: String = _
  def setMyName(name: String) {
    MyName = name
  }
  def getMyName: String = MyName

  def setTolerance(toleranceRows: java.lang.Long, tolerancePercent: Decimal) {
    if (toleranceRows == null && tolerancePercent == null)
      sys.error("Error in setTolerance: toleranceRows or tolerancePercent must have a value")
    toleranceErrorRows = toleranceRows
    toleranceErrorPercent = tolerancePercent
  }

  def setQueryLevel(value: HuemulTypeDqQueryLevel): HuemulDataQuality = {
    _queryLevel = value
    this
  }

  def setNotification(value: HuemulTypeDqNotification ): HuemulDataQuality = {
    _notification = value
    this
  }

  def setSaveErrorDetails(value: Boolean): HuemulDataQuality = {
    _saveErrorDetails = value
    this
  }
  
  
 
}