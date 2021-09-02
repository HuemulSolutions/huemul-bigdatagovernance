package com.huemulsolutions.bigdata.tables

import com.huemulsolutions.bigdata.common.HuemulTypeBigDataProvider
import com.huemulsolutions.bigdata.common.HuemulTypeBigDataProvider._
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification.HuemulTypeDqNotification
import com.huemulsolutions.bigdata.tables.HuemulTypeSecurityLevel.HuemulTypeSecurityLevel
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType.HuemulTypeStorageType
//import org.apache.log4j.Level
import org.apache.spark.sql.types._

/** A class to represent a ''Column'' from a table or modelo (Data Schema).
 *
 * Specify the `dataType`, `isRequired`, and `description` when creating a new `huemul_Column`,
 * like this:
 *
 * @example
 * {{{
 * val countryId: huemul_Columns = new huemul_Columns (IntegerType, true,"Country Identification")
 *
 * val description: huemul_Columns = new huemul_Columns (StringType, true,"Country Description")
 * }}}
 *
 * @constructor Create a new huemul_Columns with a `dataType`, `isRequired`, and `description`.
 *
 * @param dataType          Column DataType, allowed datatype see ([[https://spark.apache.org/docs/2.2.1/sql-programming-guide.html#reference]])
 * @param isRequired        Is column required (true/false)
 * @param description       Column Description
 * @param usedForCheckSum   [optional] Column use for record checksum generation (internal)
 *
 * @see For more information [[https://github.com/HuemulSolutions/huemul-bigdatagovernance/wiki/huemul_Columns]]
 *
 * @groupname column_attribute Column Attribute
 * @groupname column_data_quality Column data quality rules
 * @groupname column_mdm Column MDM Attribute
 * @groupname column_classification Column Classification Attribute
 * @groupname column_other Column Others Attribute
 */
class HuemulColumns(dataType: DataType
                    , isRequired: Boolean
                    , description: String
                    , usedForCheckSum: Boolean = true) extends Serializable {

  //val dataType: DataType = dataType
  //val description: String = description
  val required: Boolean = isRequired
  //val usedForCheckSum: Boolean = usedForCheckSumParam

  def getDataType: DataType = dataType;
  def getUsedForCheckSum: Boolean = usedForCheckSum;
  def getDescription: String = description;


  // Define if a column definition is close, not allowing any modification to its attribute
  private var _definitionIsClose: Boolean = false


  /** Set column definition to close
   * @since   1.1
   * @group   column_other
   */
  def setDefinitionIsClose() {_definitionIsClose = true}

  /** Set column definition to close
   * @since   2.6
   * @group   column_other
   */
  def getDefinitionIsClose: Boolean =  _definitionIsClose


  // ------------------------------------------------------------------------------------------------------------------
  // Utilities  -------------------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------

  /** Checks that the definition is not closed, else its throws a message in the log
   * @author  chistian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_other
   *
   * @return  true if it definition is not closed
   */
  private def checkNotCloseDefinition(propertyName:String):Boolean = {
    if (getDefinitionIsClose) {
      throw new Exception(s"You can't change properties $propertyName value on column, definition is close")
    }
    true
  }

  /** Checks that the definition is not closed and the notification is not null, else its throws a message in teh log
   * @author  chistian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_other
   *
   * @param propertyName  Notification property name (e.g. length, Decimal, etc.)
   * @param value         HuemulTypeDQNotification value
   * @return              true if valid
   */
  private def checkNotCloseAndNotNull(propertyName:String, value: Any):Boolean = {
    if (checkNotCloseDefinition(propertyName) && value == null) {
      throw new Exception(s"You can't set value of $propertyName to null on column")
    }
    true
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Primary Key column attribute -------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _isPk: Boolean = false

  /** Check if columns is Primary key
   * @since   1.1
   * @group   column_attribute
   *
   * @return  Boolean
   */
  def getIsPk: Boolean = _isPk

  /** Set column is PK
   * @since   2.1
   * @group   column_attribute
   *
   * @return  huemul_Columns
   */
  def setIsPK(): HuemulColumns = {
    if (checkNotCloseDefinition("PK"))
      _isPk = true
    this
  }


  // ------------------------------------------------------------------------------------------------------------------
  // Unique column attribute ------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _isUnique: Boolean = false
  private var _isUniqueExternalCode: String = "HUEMUL_DQ_003"
  private var _isUniqueNotification: HuemulTypeDqNotification = _

  /** Get Unique External Error Code
   * @since   2.1
   * @group   column_attribute
   *
   * @return  String Error Code
   */
  def getIsUniqueExternalCode: String = _isUniqueExternalCode

  /** Checks if column is unique
   * @since   1.1
   * @group   column_attribute
   *
   * @return  Boolean
   */
  def getIsUnique: Boolean = _isUnique

  /** Set column attribute to unique with optional error code (`externalCode`)
   *
   * @since   2.1
   * @group   column_attribute
   * @param externalCode    External Error Code String
   * @return                [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setIsUnique(externalCode: String = null): HuemulColumns = {
    if (checkNotCloseDefinition("IsUnique")) {
      _isUnique = true
      _isUniqueExternalCode = if (externalCode == null) _isUniqueExternalCode else externalCode
    }
    this
  }

  /** Set IsUnique data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return        [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def setIsUniqueNotification(notification: HuemulTypeDqNotification): HuemulColumns = {
    if (checkNotCloseAndNotNull("IsUnique",notification )) _isUniqueNotification = notification
    this
  }

  /** Get IsUnique data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @return  [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def getIsUniqueNotification:  HuemulTypeDqNotification = getDQHierarchyNotificationLevel(_isUniqueNotification)



  // ------------------------------------------------------------------------------------------------------------------
  // Nullable column attribute ----------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _nullable: Boolean = false
  private var _nullableExternalCode: String = "HUEMUL_DQ_004"
  private var _nullableNotification: HuemulTypeDqNotification = _

  /** Get Nullable External Error Code
   * @since   2.1
   * @group   column_attribute
   *
   * @return  String Error Code
   */
  def getNullableExternalCode: String = _nullableExternalCode

  /** Checks if column is nullable
   * @since   1.1
   * @group   column_attribute
   *
   * @return  Boolean
   */
  def getNullable: Boolean = _nullable

  /** Set column null attribute with optional error code (`externalCode`)
   *
   * @since   2.1
   * @group   column_attribute
   * @param externalCode    External Error Code String
   * @return                [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setNullable(externalCode: String = null): HuemulColumns = {
    if (checkNotCloseDefinition("Nullable")) {
      _nullable = true
      _nullableExternalCode = if (externalCode == null) _nullableExternalCode else externalCode
    }
    this
  }

  /** Set column null attribute with optional error code (`externalCode`)
   *
   * @since   3.0
   * @group   column_attribute
   * @param externalCode    External Error Code String
   * @return                [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setIsNull(value: Boolean, externalCode: String = null): HuemulColumns = {
    if (checkNotCloseDefinition("setIsNull")) {
      _nullable = !value
      _nullableExternalCode = if (externalCode == null) _nullableExternalCode else externalCode
    }
    this
  }


  private[tables] def setIsNullInternal(value: Boolean): HuemulColumns = {
    _nullable = !value
    this
  }



  /** Set nullable data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return        [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def setDqNullableNotification(notification: HuemulTypeDqNotification): HuemulColumns = {
    if (checkNotCloseAndNotNull("Nullable", notification))
      _nullableNotification = notification
    this
  }

  /** Get nullable data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @return  [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def getDqNullableNotification:  HuemulTypeDqNotification = getDQHierarchyNotificationLevel(_nullableNotification)



  // ------------------------------------------------------------------------------------------------------------------
  // Default Value column attribute -----------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _defaultValue: String = "null"

  /** Get the assigned default value
   * @since   2.1
   * @group   column_attribute
   *
   * @return  String default value assigned
   */
  def getDefaultValue: String = _defaultValue

  /** Set column default value
   *
   * @since   2.1
   * @group   column_attribute
   * @param value   Column default value
   * @return        [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setDefaultValues(value: String): HuemulColumns = {
    if (checkNotCloseDefinition("Default"))
      _defaultValue = value
    this
  }


  // ------------------------------------------------------------------------------------------------------------------
  // Security level column attribute ----------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // Secret, Restricted, Confidential, Intern, Departamental, Public
  private var _securityLevel: HuemulTypeSecurityLevel = HuemulTypeSecurityLevel.Public

  def getSecurityLevel: HuemulTypeSecurityLevel = _securityLevel

  /** Set column security level
   *
   * Default value is Public (Secret, Restricted, Confidential, Intern, Departamental, Public)
   *
   * @since   2.1
   * @group   column_attribute
   * @param value [[com.huemulsolutions.bigdata.tables.HuemulTypeSecurityLevel]] security level
   * @return        [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def securityLevel(value: HuemulTypeSecurityLevel): HuemulColumns = {
    if (checkNotCloseAndNotNull("SecurityLevel", value))
      _securityLevel = value
    this
  }



  // ------------------------------------------------------------------------------------------------------------------
  // Encryption type column attribute ---------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // none, sha2
  private var _encryptedType: String = "none" //none, sha2,

  /** Get encryption type
   * @since   1.1
   * @group   column_attribute
   *
   * @return  String
   */
  def getEncryptedType: String = _encryptedType

  /** Set encryption type for column
   *
   * @since   2.1
   * @group   column_attribute
   * @param value   Set type of encryption: none, sha2
   * @return        [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def encryptedType(value: String): HuemulColumns = {
    if (checkNotCloseDefinition("EncryptedType"))
      _encryptedType = value
    this
  }


  // ------------------------------------------------------------------------------------------------------------------
  // Arco data column attribute ---------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // set true for customers data that can be used for identify a unique customer (name, address, phone number)
  private var _arcoData: Boolean = false

  /** Check if column is a ARCO Data Type
   *
   * @return  Boolean
   */
  def getArcoData: Boolean = _arcoData

  /** Set column as ARCO data
   *
   * Set true for customers data that can be used for identify a unique customer (name, address, phone number)
   *
   * @since 2.1
   * @return  [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setArcoData(): HuemulColumns = {
    if (checkNotCloseDefinition("ARCO_Data"))
      _arcoData = true
    this
  }


  // ------------------------------------------------------------------------------------------------------------------
  // Business Glossary column attribute -------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // associate an external id to link with business glossary concepts
  private var _businessGlossaryId: String = ""

  /** Get business glossary id
   * @since   1.1
   * @group
   *
   * @return String
   */
  def getBusinessGlossaryId: String = _businessGlossaryId

  /** Set column business glossary asset id
   *
   * Associate an external id to link with business glossary concepts
   *
   * @since   2.1
   * @group
   * @param value   Business glossary asset id
   * @return        [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setBusinessGlossary(value: String): HuemulColumns = {
    if (checkNotCloseAndNotNull("BusinessGlossaryId",value))
      _businessGlossaryId = value
    this
  }



  // ******************************************************************************************************************
  // Data Quality Rules
  // ******************************************************************************************************************

  /** Define le notification hierarchy between attribute global notification and attribute data quality rule
   * @author  christian.sattle@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param attributeNotification  Notification level attribute data quality rule
   * @return                      Notification level
   */
  private[bigdata] def getDQHierarchyNotificationLevel(attributeNotification: HuemulTypeDqNotification) :HuemulTypeDqNotification =
    if (attributeNotification == null) getDqNotification else attributeNotification

  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MIN length -----------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _dqMinLen: Integer = _
  private var _dqMinLenExternalCode: String = "HUEMUL_DQ_006"
  private var _dqMinLenNotification: HuemulTypeDqNotification = _

  /** Get data quality min length error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return String error code
   */
  def getDqMinLenExternalCode: String = _dqMinLenExternalCode

  /** Get data quality min length value
   * @group   column_data_quality
   *
   * @return  Min length value
   */
  def getDqMinLen: Integer = _dqMinLen

  /** Set data quality min length value
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Min length value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setDqMinLen(value: Integer, externalCode: String = null): HuemulColumns = {
    if (checkNotCloseDefinition("DQ min length")){
      _dqMinLen = value
      _dqMinLenExternalCode = if (externalCode==null) _dqMinLenExternalCode else externalCode
    }
    this
  }

  /** Set min len data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return        [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def setDqMinLenNotification(notification: HuemulTypeDqNotification): HuemulColumns = {
    if (checkNotCloseAndNotNull("DQ min length", notification))
      _dqMinLenNotification = notification
    this
  }

  /** Get min len data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @return  [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def getDqMinLenNotification:  HuemulTypeDqNotification = getDQHierarchyNotificationLevel(_dqMinLenNotification)



  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MAX length -----------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _dqMaxLen: Integer = _
  private var _dqMaxLenExternalCode: String = "HUEMUL_DQ_006"
  private var _DqMaxLenNotification: HuemulTypeDqNotification = _

  /** Get data quality max length error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return String error code
   */
  def getDqMaxLenExternalCode: String = _dqMaxLenExternalCode

  /** Get data quality max length value
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  max length value
   */
  def getDqMaxLen: Integer = _dqMaxLen

  /** Set data quality max length value
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Max length value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setDqMaxLen(value: Integer, externalCode: String = null): HuemulColumns = {
    if (checkNotCloseDefinition("DQ max length")) {
      _dqMaxLen = value
      _dqMaxLenExternalCode = if (externalCode == null) _dqMaxLenExternalCode else externalCode
    }
    this
  }

  /** Set max len data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return              [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def setDqMaxLenNotification(notification: HuemulTypeDqNotification): HuemulColumns = {
    if (checkNotCloseAndNotNull("DQ max length",notification))
      _DqMaxLenNotification = notification
    this
  }

  /** Get max len data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @return  [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def getDqMaxLenNotification: HuemulTypeDqNotification = getDQHierarchyNotificationLevel(_DqMaxLenNotification)



  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MIN decimal value ----------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _dqMinDecimalValue: Decimal = _
  private var _dqMinDecimalValueExternalCode: String = "HUEMUL_DQ_007"
  private var _dqMinDecimalValueNotification: HuemulTypeDqNotification = _

  /** Get data quality min decimal value error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  String error code
   */
  def getDqMinDecimalValueExternalCode: String = _dqMinDecimalValueExternalCode

  /** Get data quality min decimal value
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  Min decimal value
   */
  def getDqMinDecimalValue: Decimal = _dqMinDecimalValue

  /** Set data quality min decimal value
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Min decimal value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setDqMinDecimalValue(value: Decimal, externalCode: String = null): HuemulColumns = {
    if (checkNotCloseDefinition("DQ min decimal")) {
      _dqMinDecimalValue = value
      _dqMinDecimalValueExternalCode = if (externalCode == null) _dqMinDecimalValueExternalCode else externalCode
    }
    this
  }

  /** Set min decimal value data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return        [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def setDqMinDecimalValueNotification(notification: HuemulTypeDqNotification): HuemulColumns = {
    if (checkNotCloseAndNotNull("DQ min length",notification))
      _dqMinDecimalValueNotification = notification

    this
  }

  /** Get min decimal value data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @return  [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def getDqMinDecimalValueNotification: HuemulTypeDqNotification = getDQHierarchyNotificationLevel(_dqMinDecimalValueNotification)



  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MAX decimal value ----------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _dqMaxDecimalValue: Decimal = _
  private var _dqMaxDecimalValueExternalCode: String = "HUEMUL_DQ_007"
  private var _dqMaxDecimalValueNotification: HuemulTypeDqNotification = _

  /** Get data quality max decimal value error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  String error code
   */
  def getDqMaxDecimalValueExternalCode: String = _dqMaxDecimalValueExternalCode

  /** Get data quality max decimal value
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  Max decimal value
   */
  def getDqMaxDecimalValue: Decimal = _dqMaxDecimalValue

  /** Set data quality max decimal value
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Max decimal value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setDqMaxDecimalValue(value: Decimal, externalCode: String = null): HuemulColumns = {
    if (checkNotCloseDefinition("DQ max decimal")) {
      _dqMaxDecimalValue = value
      _dqMaxDecimalValueExternalCode = if (externalCode == null) _dqMaxDecimalValueExternalCode else externalCode
    }
    this
  }

  /** Set max decimal data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return              [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def setDqMaxDecimalValueNotification(notification: HuemulTypeDqNotification): HuemulColumns = {
    if (checkNotCloseAndNotNull("DQ max decimal", notification))
      _dqMaxDecimalValueNotification = notification
    this
  }

  /** Get max decimal data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @return  [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def getDqMaxDecimalValueNotification: HuemulTypeDqNotification = getDQHierarchyNotificationLevel(_dqMaxDecimalValueNotification)



  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MIN DateTime value ---------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _dqMinDateTimeValue: String = _
  private var _dqMinDateTimeValueExternalCode: String = "HUEMUL_DQ_008"
  private var _dqMinDateTimeValueNotification: HuemulTypeDqNotification = _

  /** Get data quality min date time value error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  String error code
   */
  def getDqMinDateTimeValueExternalCode: String = _dqMinDateTimeValueExternalCode

  /** Get data quality min date time value
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  Min date time value
   */
  def getDqMinDateTimeValue: String = _dqMinDateTimeValue

  /** Set data quality min date time value
   *
   * Format: YYYY-MM-DD HH:mm:ss or fieldName
   *
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Min date time value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setDqMinDateTimeValue(value: String, externalCode: String = null): HuemulColumns = {
    if (checkNotCloseDefinition("DQ min datetime")) {
      _dqMinDateTimeValue = value
      _dqMinDateTimeValueExternalCode = if (externalCode == null) _dqMinDateTimeValueExternalCode  else externalCode
    }
    this
  }

  /** Set min datetime value data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return              [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def setDqMinDateTimeValueNotification(notification: HuemulTypeDqNotification): HuemulColumns = {
    if (checkNotCloseAndNotNull("DQ min datetime", notification))
      _dqMinDateTimeValueNotification = notification
    this
  }

  /** Get min datetime value data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @return  [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def getDqMinDateTimeValueNotification: HuemulTypeDqNotification = getDQHierarchyNotificationLevel(_dqMinDateTimeValueNotification)



  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MAX DateTime value ---------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _dqMaxDateTimeValue: String = _
  private var _dqMaxDateTimeValueExternalCode: String = "HUEMUL_DQ_008"
  private var _dqMaxDateTimeValueNotification: HuemulTypeDqNotification = _

  /** Get data quality max date time value error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  String error code
   */
  def getDqMaxDateTimeValueExternalCode: String = _dqMaxDateTimeValueExternalCode

  /** Get data quality max date time value
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  Max date time value
   */
  def getDqMaxDateTimeValue: String = _dqMaxDateTimeValue

  /** Set data quality max date time value
   *
   * Format: YYYY-MM-DD HH:mm:ss or fieldName
   *
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Max date time value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setDqMaxDateTimeValue(value: String, externalCode: String = null): HuemulColumns = {
    if (checkNotCloseDefinition("DQ max datetime")) {
      _dqMaxDateTimeValue = value
      _dqMaxDateTimeValueExternalCode = if (externalCode == null )  _dqMaxDateTimeValueExternalCode else externalCode
    }
    this
  }

  /** Set max datetime value data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return              [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def setDqMaxDateTimeValueNotification(notification: HuemulTypeDqNotification): HuemulColumns = {
    if (checkNotCloseAndNotNull("DQ max datetime", notification))
      _dqMaxDateTimeValueNotification = notification
    this
  }

  /** Get min datetime value data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @return  [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def getDqMaxDateTimeValueNotification: HuemulTypeDqNotification = getDQHierarchyNotificationLevel(_dqMaxDateTimeValueNotification)




  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality Regular Expression value ---------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // ToDo: Validate Regular Expression
  private var _dqRegExp: String = _
  private var _dqRegExpExternalCode: String = "HUEMUL_DQ_005"
  private var _dqRegExpNotification: HuemulTypeDqNotification = _

  /** Get data quality regular expression error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  String error code
   */
  def getDqRegExpressionExternalCode: String = _dqRegExpExternalCode

  /** Get data quality regular expression
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  Max date time value
   */
  def getDqRegExpression: String = _dqRegExp

  /** Set data quality regular expression
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Regular expression statement
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setDqRegExpression(value: String, externalCode: String = null): HuemulColumns = {
    if (checkNotCloseDefinition("DQ RegExpression")) {
      _dqRegExp = value
      _dqRegExpExternalCode = if (externalCode == null) _dqRegExpExternalCode  else externalCode
    }
    this
  }


  /** Set max datetime value data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return              [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setDqRegExpressionNotification(notification: HuemulTypeDqNotification): HuemulColumns = {
    if (checkNotCloseAndNotNull("DQ RegExpression", notification))
      _dqRegExpNotification = notification
    this
  }

  /** Get min datetime value data quality rule notification
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @return  [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def getDqRegExpressionNotification: HuemulTypeDqNotification = getDQHierarchyNotificationLevel(_dqRegExpNotification)


  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality notification ---------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // Column Data Rule Notification Level ERROR(default), WARNING or WARNING_EXCLUDE for all column data rules
  private var _dqNotification: HuemulTypeDqNotification = HuemulTypeDqNotification.ERROR

  /** Set de Error Notification Level for the column data quality for all column data rules
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @param value   Notification Level ERROR (default), WARNING or WARNING_EXCLUDE
   * @return        [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def setDqNotification(value: HuemulTypeDqNotification ): HuemulColumns = {
    _dqNotification = value
    this
  }

  /** Get de Error Notification Level for the column data quality for all column data rules
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   * @return  [[com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification]]
   */
  def getDqNotification: HuemulTypeDqNotification = _dqNotification

  // ******************************************************************************************************************
  // MDM Properties
  // ******************************************************************************************************************

  // ------------------------------------------------------------------------------------------------------------------
  // MDM Enable old value full trace ----------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // save all old values trace in other table (default disable)
  private var _mdmEnableOldValueFullTrace: Boolean = false

  /** Check if MDM old value full trace is enabled
   * @since   1.1
   * @group   column_mdm
   *
   * @return  Boolean
   */
  def getMdmEnableOldValueFullTrace: Boolean = _mdmEnableOldValueFullTrace

  /** Set MDM old value full trace
   *
   * Save all old values trace in other table (default false)
   *
   * @since   2.1
   * @group   column_mdm
   *
   * @return  [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setMdmEnableOldValueFullTrace(): HuemulColumns = {
    if (checkNotCloseDefinition("MDM EnableOldValue FullTrace"))
      _mdmEnableOldValueFullTrace = true
    this
  }



  // ------------------------------------------------------------------------------------------------------------------
  // MDM Enable old value ---------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // save old value when new value arrive
  private var _mdmEnableOldValue: Boolean = false

  /** Check if MDM old value is enabled
   * @since   1.4
   * @group   column_mdm
   *
   * @return  Boolean
   */
  def getMdmEnableOldValue: Boolean = _mdmEnableOldValue


  /** Set MDM old value full trace.
   *
   * Save old value when new value arrive (default false)
   *
   * @since   2.1
   * @group   column_mdm
   *
   * @return  [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setMdmEnableOldValue(): HuemulColumns = {
    if (checkNotCloseDefinition("MDM EnableOldValue"))
      _mdmEnableOldValue = true
    this
  }




  // ------------------------------------------------------------------------------------------------------------------
  // MDM Enable old value add datetime column -------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // create a new field with datetime log for change value
  private var _mdmEnableDTLog: Boolean = false

  /** Check if MDM old value Datetime log is enabled
   * @since   1.4
   * @group   column_mdm
   *
   * @return  Boolean
   */
  def getMdmEnableDTLog: Boolean = _mdmEnableDTLog

  /** Enable MDM old value datetime log on additional column/field.
   *
   * create a new field with datetime log for change value (default false)
   *
   * @since   2.1
   * @group   column_mdm
   *
   * @return  [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setMdmEnableDTLog(): HuemulColumns = {
    if (checkNotCloseDefinition("MDM EnableDTLog"))
      _mdmEnableDTLog = true
    this
  }


  // ------------------------------------------------------------------------------------------------------------------
  // MDM Enable old value add datetime column -------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // Create a new field with Process Log for change value
  private var _mdmEnableProcessLog: Boolean = false

  /** Check if MDM old value process name log is enabled
   * @since   1.4
   * @group   column_mdm
   *
   * @return  Boolean
   */
  def getMdmEnableProcessLog: Boolean = _mdmEnableProcessLog

  /** Enable MDM old value datetime log on additional column/field.
   *
   * Create a new field with Process Name, who did the change value (default false)
   *
   * @since   2.1
   * @group   column_mdm
   *
   * @return  [[com.huemulsolutions.bigdata.tables.HuemulColumns]]
   */
  def setMdmEnableProcessLog(): HuemulColumns = {
    if (checkNotCloseDefinition("MDM EnableProcessLog"))
      _mdmEnableProcessLog = true
    this
  }


  // ------------------------------------------------------------------------------------------------------------------
  // User mapping attributes
  // ------------------------------------------------------------------------------------------------------------------
  private var _mappedName: String = _
  private var _myName: String = _
  private var _replaceValueOnUpdate: Boolean = false
  private var _sqlForUpdate: String = _
  private var _sqlForInsert: String = _

  /** Set Column Name
   *
   * @param name  Column Name
   */
  private[bigdata] def setMyName(name: String) {
    this._myName = name
  }

  /**
   *
   * @param tableStorage [[com.huemulsolutions.bigdata.tables.HuemulTypeStorageType]] Identify storage type PARQUET, etc.
   * @return
   */
  def getMyName(tableStorage: HuemulTypeStorageType): String = {
    if (this._myName == null || this._myName == "")
      throw new Exception(s"Huemul ERROR: MUST call 'ApplyTableDefinition' in table definition, (field description: ${this.description} )")

    getCaseType(tableStorage,this._myName)
  }


  /** Convert column name caso for compatibility between table storage's
   *
   * ToDo: replicated en huemul_BigDataGovernance -> evaluate to mive sperata Object class (static class)
   *
   * @param tableStorage [[com.huemulsolutions.bigdata.tables.HuemulTypeStorageType]] Identify storage type PARQUET, etc.
   * @param value        Data to be convert the case
   * @return              String
   */
  def getCaseType(tableStorage: HuemulTypeStorageType, value: String): String =
    if (tableStorage == HuemulTypeStorageType.AVRO) value.toLowerCase() else value

  /** Get the mapping name
   *
   * @return String
   */
  def getMappedName: String = this._mappedName



  /**
   *
   * @return
   */
  def getReplaceValueOnUpdate: Boolean = this._replaceValueOnUpdate



  /**
   *
   * @return
   */
  def getSQLForUpdate: String = this._sqlForUpdate

  /**
   *
   * @return
   */
  def getSQLForInsert: String = this._sqlForInsert


  /** Set mapping DataFrame fields to Table Fields for insert and update.
   *
   * For SQLForUpdate and SQLForInsert, in both cases, don't include "as Namefield" instead "old" is a reference to
   * the table and "new" is reference to your DataFrame
   *
   * @param param_name            fields name in DataFrame
   * @param replaceValueOnUpdate  true (default) for replace values in table
   * @param sqlForUpdate          example: cast(case when old.[field] > new.[field] then old.[field] else new.[field] end as Integer)
   * @param sqlForInsert          example: cast(case when old.[field] > new.[field] then old.[field] else new.[field] end as Integer)
   */
  def setMapping(param_name: String, replaceValueOnUpdate: Boolean = true, sqlForUpdate: String = null, sqlForInsert: String = null) {
    this._mappedName = param_name
    this._replaceValueOnUpdate = replaceValueOnUpdate
    this._sqlForUpdate = sqlForUpdate
    this._sqlForInsert = sqlForInsert
  }



  /** Get DataType to be deployed
   *
   * Return data type with AVRO and spark < 2.4 compatibility
   *
   * @since 2.5
   * @param bigDataProvider [[com.huemulsolutions.bigdata.common.HuemulTypeBigDataProvider]] Identify Data Provider like Databrick
   * @param storageType     [[com.huemulsolutions.bigdata.tables.HuemulTypeStorageType]] Identify storage type PARQUET
   *                        , ORC, AVRO, HBASE, DELTA
   * @return                  DataType
   */
  def getDataTypeDeploy(bigDataProvider: HuemulTypeBigDataProvider, storageType: HuemulTypeStorageType ): DataType = {
    var result = this.dataType
    if (storageType == HuemulTypeStorageType.AVRO
      && bigDataProvider != HuemulTypeBigDataProvider.databricks
      && ( result.sql.toUpperCase().contains("DATE") || result.sql.toUpperCase().contains("TIMESTAMP"))
    ) {
      result = StringType
    } else if (storageType == HuemulTypeStorageType.AVRO
      && bigDataProvider == HuemulTypeBigDataProvider.databricks
      && result.sql.toUpperCase().contains("SMALLINT")
    ) {
      result = IntegerType
    }
    result
  }

  // ------------------------------------------------------------------------------------------------------------------
  // HBase attributes
  // ------------------------------------------------------------------------------------------------------------------
  //from 2.2 --> add setting to HBase
  private var _hiveDf: String = "default"
  private var _hiveCol: String = _

  /** Define family and column name on HBase Tables
   *
   * @param df dataframe
   * @param col column name
   * @return
   */
  def setHBaseCatalogMapping(df: String, col: String = null): HuemulColumns = {
    _hiveDf = df
    _hiveCol = col
    this
  }

  /**
   *
   * @return
   */
  def getHBaseCatalogFamily: String = _hiveDf

  /**
   *
   * @return
   */
  def getHBaseCatalogColumn: String = if (_hiveCol == null) getMyName(HuemulTypeStorageType.HBASE) else _hiveCol

  /** Get hbase data type
   *
   * @return  String data type
   */
  def getHBaseDataType: String = {
    if (dataType == DataTypes.StringType) "string"
    else if (dataType == DataTypes.IntegerType) "int"
    else if (dataType == DataTypes.ShortType) "smallint"    //TODO: Repetido ShortType
    else if (dataType == DataTypes.BooleanType) "boolean"
    else if (dataType == DataTypes.DoubleType) "double"
    else if (dataType == DataTypes.FloatType) "float"
    else if (dataType == DataTypes.LongType) "bigint"
    else if (dataType == DataTypes.ShortType) "tinyint"     //TODO: Repetido ShortType
    else if (dataType == DataTypes.BinaryType) "binary"
    else "string"
  }

  //from 2.6
  private var _partitionPosition: Integer = 0
  private var _partitionDropBeforeSave: Boolean = true
  private var _partitionOneValuePerProcess: Boolean = true
  def getPartitionColumnPosition: Integer = _partitionPosition
  def getPartitionDropBeforeSave: Boolean = _partitionDropBeforeSave
  def getPartitionOneValuePerProcess: Boolean = _partitionOneValuePerProcess
  def setPartitionColumn(position: Integer, dropBeforeInsert: Boolean = true, oneValuePerProcess: Boolean = true): HuemulColumns = {
    if (getDefinitionIsClose) {
      sys.error("You can't change value of setPartitionColumn, definition is close")
      return this
    }
   
    if (position <= 0) {
      sys.error("position value must be >= 1")
      return this
    }

    _partitionPosition = position
    _partitionDropBeforeSave = dropBeforeInsert
    _partitionOneValuePerProcess = oneValuePerProcess
    
    this
  }
  
 
}
