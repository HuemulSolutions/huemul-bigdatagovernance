package com.huemulsolutions.bigdata.dataquality

import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.tables.HuemulColumns
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDQQueryLevel._
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDQNotification._

/** huemul_DataQuality permite ejecutar validaciones en la misma tabla, ya sea
 *  a nivel de fila para un campo (campo_a > 10), comparar varios campos (campo_a > campo_b)
 *  o a nivel agrupado (sum(campo_a) > sum(campo_b)
 *  @constructor Inicializa variables en null
 *  @param FieldName nombre del campo de la validación, puede ser null si la validación es a nivel de tabla
 *  @param Description descripción de la validación
 *  @param sqlformula formula sql en positivo (ejemplo campo_a > campo_b para validar que campo_a debe ser mayor a campo_b)
 *  @param Error_Code codigo de error, debe usar códigos que vayan entre 1 y 999
 *  @param QueryLevel indica si es por fila (row) o agrupado (aggregate). por default es Row
 *  @param Notification nivel de notificación, ERROR gatilla error y fuerza salida, WARNING es solo una alerta. por default es ERROR
 *  @param SaveErrorDetails (default true) indica si guarda el detalle del error o warning de DQ en tabla de detalle
 *  @param DQ_ExternalCode (Default null) indica el código externo de tabla de DQ
 */
class HuemulDataQuality(FieldName: HuemulColumns
                        , Description: String
                        , sqlformula: String
                        , Error_Code: Integer
                        , QueryLevel: HuemulTypeDQQueryLevel = HuemulTypeDQQueryLevel.Row //,IsAggregated: Boolean
                        , Notification: HuemulTypeDQNotification = HuemulTypeDQNotification.ERROR //RaiseError: Boolean
                        , SaveErrorDetails: Boolean = true
                        , DQ_ExternalCode: String = null
            ) extends Serializable {

  private var _DQ_ExternalCode: String = DQ_ExternalCode
  private var ToleranceError_Percent: Decimal = _
  private var _QueryLevel: HuemulTypeDQQueryLevel = QueryLevel
  private var _Notification: HuemulTypeDQNotification = Notification
  private var _SaveErrorDetails: Boolean = SaveErrorDetails


  /**% of total for refuse validation. Example: 0.15 = 15% (null to not use)
   */
  def getToleranceError_Percent: Decimal = ToleranceError_Percent

  private var ToleranceError_Rows: java.lang.Long = 0
  /**N° of records for refuse validation. Example: 1000 = 1000 rows with error  (null to not use)
   */
  def getToleranceError_Rows: java.lang.Long = ToleranceError_Rows

  /**SQL for validation, expressed in a positive way (boolean) . Example: Field1 < Field2 (field oK)
   */
  def getSQLFormula: String = sqlformula //= null

  private var _Id: Integer = _
  def setId(Id: Integer) {_Id = Id}
  def getId: Integer = _Id




  def getFieldName: HuemulColumns = FieldName
  def getQueryLevel: HuemulTypeDQQueryLevel  = _QueryLevel
  def getDescription: String  = Description
  def getNotification: HuemulTypeDQNotification = _Notification
  def getSaveErrorDetails: Boolean = {if (_QueryLevel == HuemulTypeDQQueryLevel.Row) _SaveErrorDetails else false}
  def getErrorCode: Integer = Error_Code
  def setDQ_ExternalCode(value: String) {_DQ_ExternalCode = value }
  def getDQ_ExternalCode: String =  _DQ_ExternalCode
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
    ToleranceError_Rows = toleranceRows
    ToleranceError_Percent = tolerancePercent
  }

  def setQueryLevel(value: HuemulTypeDQQueryLevel): HuemulDataQuality = {
    _QueryLevel = value
    this
  }

  def setNotification(value: HuemulTypeDQNotification ): HuemulDataQuality = {
    _Notification = value
    this
  }

  def setSaveErrorDetails(value: Boolean): HuemulDataQuality = {
    _SaveErrorDetails = value
    this
  }
  
  
 
}