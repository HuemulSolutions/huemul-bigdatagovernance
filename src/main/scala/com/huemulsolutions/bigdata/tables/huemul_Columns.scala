package com.huemulsolutions.bigdata.tables

import com.huemulsolutions.bigdata.common.huemulType_bigDataProvider
import com.huemulsolutions.bigdata.common.huemulType_bigDataProvider._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification.huemulType_DQNotification
import com.huemulsolutions.bigdata.tables.huemulType_SecurityLevel.huemulType_SecurityLevel
import com.huemulsolutions.bigdata.tables.huemulType_StorageType.huemulType_StorageType
import org.apache.log4j.Level
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
class huemul_Columns(dataType: DataType
                     , isRequired: Boolean
                     , description: String
                     , usedForCheckSum: Boolean = true) extends Serializable {

  val DataType: DataType = dataType
  val Description: String = description
  val Required: Boolean = isRequired
  val UsedForCheckSum: Boolean = usedForCheckSum


  // Define if a column definition is close, not allowing any modification to its attribute
  private var _DefinitionIsClose: Boolean = false

  /** Set column definition to close
   * @since   1.1
   * @group   column_other
   */
  @deprecated("this method will be removed, instead use setDefinitionIsClose(): huemul_Columns", "3.0")
  def SetDefinitionIsClose() {_DefinitionIsClose = true}

  /** Set column definition to close
   * @since   1.1
   * @group   column_other
   */
  def setDefinitionIsClose() {_DefinitionIsClose = true}

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
    if (_DefinitionIsClose) {
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
   * @param value         huemulType_DQNotification value
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
  private var _IsPK: Boolean = false

  /** Check if columns is Primary key
   * @since   1.1
   * @group   column_attribute
   *
   * @return  Boolean
   */
  def getIsPK: Boolean = _IsPK

  /** Set column is PK
   * @since   2.1
   * @group   column_attribute
   *
   * @return  huemul_Columns
   */
  def setIsPK(): huemul_Columns = {
    if (checkNotCloseDefinition("PK"))
      _IsPK = true
    this
  }

  /** Set column is PK
   * @since   1.1
   * @group   column_attribute
   *
   * @return  huemul_Columns
   */
  @deprecated("this method will be removed, instead use setIsPK(): huemul_Columns", "3.0")
  def setIsPK(value: Boolean): Unit = {
    if (checkNotCloseAndNotNull("PK",value))
      _IsPK = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Unique column attribute ------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _IsUnique: Boolean = false
  private var _IsUniqueExternalCode: String = "HUEMUL_DQ_003"
  private var _IsUniqueNotification: huemulType_DQNotification = _

  /** Get Unique External Error Code
   * @since   2.1
   * @group   column_attribute
   *
   * @return  String Error Code
   */
  def getIsUnique_externalCode: String = _IsUniqueExternalCode

  /** Checks if column is unique
   * @since   1.1
   * @group   column_attribute
   *
   * @return  Boolean
   */
  def getIsUnique: Boolean = _IsUnique

  /** Set column attribute to unique with optional error code (`externalCode`)
   * @since   2.1
   * @group   column_attribute
   *
   * @param externalCode    External Error Code String
   * @return                [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setIsUnique(externalCode: String = null): huemul_Columns = {
    if (checkNotCloseDefinition("IsUnique")) {
      _IsUnique = true
      _IsUniqueExternalCode = if (externalCode == null) _IsUniqueExternalCode else externalCode
    }
    this
  }

  /** Set IsUnique data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return        [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def setIsUnique_Notification(notification: huemulType_DQNotification): huemul_Columns = {
    if (checkNotCloseAndNotNull("IsUnique",notification )) _IsUniqueNotification = notification
    this
  }

  /** Get IsUnique data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @return  [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def getIsUnique_Notification:  huemulType_DQNotification = _IsUniqueNotification


  /** Set column attribute to unique
   * @since   1.1
   * @group   column_attribute
   *
   * @param value true/false
   */
  @deprecated("this method will be removed, instead use setIsUnique(externalCode:String = null): huemul_Columns", "3.0")
  def setIsUnique(value: Boolean) {
    if (checkNotCloseAndNotNull("IsUnique" , value))
      _IsUnique = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Nullable column attribute ----------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _Nullable: Boolean = false
  private var _NullableExternalCode: String = "HUEMUL_DQ_004"
  private var _NullableNotification: huemulType_DQNotification = _

  /** Get Nullable External Error Code
   * @since   2.1
   * @group   column_attribute
   *
   * @return  String Error Code
   */
  def getNullable_externalCode: String = _NullableExternalCode

  /** Checks if column is nullable
   * @since   1.1
   * @group   column_attribute
   *
   * @return  Boolean
   */
  def getNullable: Boolean = _Nullable

  /** Set column null attribute with optional error code (`externalCode`)
   * @since   2.1
   * @group   column_attribute
   *
   * @param externalCode    External Error Code String
   * @return                [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setNullable(externalCode: String = null): huemul_Columns = {
    if (checkNotCloseDefinition("Nullable")) {
      _Nullable = true
      _NullableExternalCode = if (externalCode == null) _NullableExternalCode else externalCode
    }
    this
  }

  /** Set nullable data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return        [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def setDQ_Nullable_Notification(notification: huemulType_DQNotification): huemul_Columns = {
    if (checkNotCloseAndNotNull("Nullable", notification))
      _NullableNotification = notification
    this
  }

  /** Get nullable data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @return  [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def getDQ_Nullable_Notification:  huemulType_DQNotification = _NullableNotification

  /** Set column null attribute
   * @since   1.1
   * @group   column_attribute
   *
   * @param value true/false
   */
  @deprecated("this method will be removed, instead use setNullable(externalCode:String = null): huemul_Columns", "3.0")
  def setNullable(value: Boolean) {
    if (_DefinitionIsClose && value)
      sys.error("You can't change value of setNullable, definition is close")
    else
      _Nullable = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Default Value column attribute -----------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _DefaultValue: String = "null"

  /** Get the assigned default value
   * @since   2.1
   * @group   column_attribute
   *
   * @return  String default value assigned
   */
  def getDefaultValue: String = _DefaultValue

  /** Set column default value
   * @since   2.1
   * @group   column_attribute
   *
   * @param value   Column default value
   * @return        [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setDefaultValues(value: String): huemul_Columns = {
    if (checkNotCloseDefinition("Default"))
      _DefaultValue = value
    this
  }

  /** Set column default value
   * @since   1.1
   * @group   column_attribute
   *
   * @param value   Column default value
   */
  @deprecated("this method will be removed, instead use setDefaultValues(value: String): huemul_Columns", "3.0")
  def setDefaultValue(value: String) {
    if (checkNotCloseDefinition("Default"))
      _DefaultValue = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Security level column attribute ----------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // Secret, Restricted, Confidential, Intern, Departamental, Public
  private var _SecurityLevel: huemulType_SecurityLevel = huemulType_SecurityLevel.Public

  def getSecurityLevel: huemulType_SecurityLevel = _SecurityLevel

  /** Set column security level
   *
   * Default value is Public (Secret, Restricted, Confidential, Intern, Departamental, Public)
   *
   * @since   2.1
   * @group   column_attribute
   *
   * @param value   [[com.huemulsolutions.bigdata.tables.huemulType_SecurityLevel]] security level
   * @return        [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def securityLevel(value: huemulType_SecurityLevel): huemul_Columns = {
    if (checkNotCloseAndNotNull("SecurityLevel", value))
      _SecurityLevel = value
    this
  }

  /** Set column security level
   * @since   1.4
   * @group   column_attribute
   *
   * @param value   [[com.huemulsolutions.bigdata.tables.huemulType_SecurityLevel]] security level
   */
  @deprecated("this method will be removed, instead use securityLevel(value: huemulType_SecurityLevel): huemul_Columns", "3.0")
  def setSecurityLevel(value: huemulType_SecurityLevel) {
    if (checkNotCloseAndNotNull("SecurityLevel", value))
      _SecurityLevel = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Encryption type column attribute ---------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // none, sha2
  private var _EncryptedType: String = "none" //none, sha2,

  /** Get encryption type
   * @since   1.1
   * @group   column_attribute
   *
   * @return  String
   */
  def getEncryptedType: String = _EncryptedType

  /** Set encryption type for column
   * @since   2.1
   * @group   column_attribute
   *
   * @param value   Set type of encryption: none, sha2
   * @return        [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def encryptedType(value: String): huemul_Columns = {
    if (checkNotCloseDefinition("EncryptedType"))
      _EncryptedType = value
    this
  }

  /** Set encryption type for column
   * @since   2.1
   * @group   column_attribute
   *
   * @param value   Set type of encryption: none, sha2
   */
  @deprecated("this method will be removed, instead use encryptedType(value: String): huemul_Columns", "3.0")
  def setEncryptedType(value: String) {
    if (checkNotCloseAndNotNull("EncryptedType",value))
      _EncryptedType = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Arco data column attribute ---------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // set true for customers data that can be used for identify a unique customer (name, address, phone number)
  private var _ArcoData: Boolean = false

  /** Check if column is a ARCO Data Type
   *
   * @return  Boolean
   */
  def getARCO_Data: Boolean = _ArcoData

  /** Set column as ARCO data
   *
   * Set true for customers data that can be used for identify a unique customer (name, address, phone number)
   *
   * @since 2.1
   *
   * @return  [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setARCO_Data(): huemul_Columns = {
    if (checkNotCloseDefinition("ARCO_Data"))
      _ArcoData = true
    this
  }

  /** Set column as ARCO data
   *
   * @param value   true/false
   */
  @deprecated("this method will be removed, instead use setARCO_Data(): huemul_Columns", "3.0")
  def setARCO_Data(value: Boolean) {
    if (checkNotCloseAndNotNull("ARCO_Data",value))
      _ArcoData = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Business Glossary column attribute -------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // associate an external id to link with business glossary concepts
  private var _BusinessGlossaryId: String = ""

  /** Get business glossary id
   * @since   1.1
   * @group
   *
   * @return String
   */
  def getBusinessGlossary_Id: String = _BusinessGlossaryId

  /** Set column business glossary asset id
   *
   * Associate an external id to link with business glossary concepts
   *
   * @since   2.1
   * @group
   *
   * @param value   Business glossary asset id
   * @return        [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setBusinessGlossary(value: String): huemul_Columns = {
    if (checkNotCloseAndNotNull("BusinessGlossaryId",value))
      _BusinessGlossaryId = value
    this
  }

  /** Set column business glossary asset id
   *
   * @since   2.1
   * @group
   *
   * @param value   Business glossary asset id
   */
  @deprecated("this method will be removed, instead use setBusinessGlossary(value: String): huemul_Columns", "3.0")
  def setBusinessGlossary_Id(value: String) {
    if (checkNotCloseAndNotNull("BusinessGlossaryId",value))
      _BusinessGlossaryId = value
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
  private[bigdata] def getDQHierarchyNotificationLevel(attributeNotification: huemulType_DQNotification) :huemulType_DQNotification =
    if (attributeNotification == null) getDQ_NotificationX else attributeNotification

  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MIN length -----------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _DQMinLen: Integer = _
  private var _DQMinLenExternalCode: String = "HUEMUL_DQ_006"
  private var _DQMinLenNotification: huemulType_DQNotification = _

  /** Get data quality min length error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return String error code
   */
  def getDQ_MinLen_externalCode: String = _DQMinLenExternalCode

  /** Get data quality min length value
   * @group   column_data_quality
   *
   * @return  Min length value
   */
  def getDQ_MinLen: Integer = _DQMinLen

  /** Set data quality min length value
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Min length value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setDQ_MinLen(value: Integer, externalCode: String = null): huemul_Columns = {
    if (checkNotCloseDefinition("DQ min length")){
      _DQMinLen = value
      _DQMinLenExternalCode = if (externalCode==null) _DQMinLenExternalCode else externalCode
    }
    this
  }

  /** Set min len data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return        [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def setDQ_MinLen_Notification(notification: huemulType_DQNotification): huemul_Columns = {
    if (checkNotCloseAndNotNull("DQ min length", notification))
      _DQMinLenNotification = notification
    this
  }

  /** Get min len data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @return  [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def getDQ_MinLen_Notification:  huemulType_DQNotification = _DQMinLenNotification

  /** Set data quality min length value
   * @since   1.1
   * @group   column_data_quality
   *
   * @param value   Min length value
   */
  @deprecated("this method will be removed, instead use setDQ_MinLen(value: Integer, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MinLen(value: Integer) {
    if (checkNotCloseDefinition("DQ min length"))
      _DQMinLen = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MAX length -----------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _DQMaxLen: Integer = _
  private var _DQMaxLenExternalCode: String = "HUEMUL_DQ_006"
  private var _DQMaxLenNotification: huemulType_DQNotification = _

  /** Get data quality max length error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return String error code
   */
  def getDQ_MaxLen_externalCode: String = _DQMaxLenExternalCode

  /** Get data quality max length value
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  max length value
   */
  def getDQ_MaxLen: Integer = _DQMaxLen

  /** Set data quality max length value
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Max length value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setDQ_MaxLen(value: Integer, externalCode: String = null): huemul_Columns = {
    if (checkNotCloseDefinition("DQ max length")) {
      _DQMaxLen = value
      _DQMaxLenExternalCode = if (externalCode == null) _DQMaxLenExternalCode else externalCode
    }
    this
  }

  /** Set max len data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return              [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def setDQ_MaxLen_Notification(notification: huemulType_DQNotification): huemul_Columns = {
    if (checkNotCloseAndNotNull("DQ max length",notification))
      _DQMaxLenNotification = notification
    this
  }

  /** Get max len data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @return  [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def getDQ_MaxLen_Notification: huemulType_DQNotification = _DQMaxLenNotification

  /** Set data quality max length value
   * @author  christian.sattler@gmail.com
   * @since   1.1
   * @group   column_data_quality
   *
   * @param value   Max length value
   */
  @deprecated("this method will be removed, instead use setDQ_MaxLen(value: Integer, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MaxLen(value: Integer) {
    if (checkNotCloseDefinition("DQ max length"))
      _DQMaxLen = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MIN decimal value ----------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _DQMinDecimalValue: Decimal = _
  private var _DQMinDecimalValueExternalCode: String = "HUEMUL_DQ_007"
  private var _DQMinDecimalValueNotification: huemulType_DQNotification = _

  /** Get data quality min decimal value error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  String error code
   */
  def getDQ_MinDecimalValue_externalCode: String = _DQMinDecimalValueExternalCode

  /** Get data quality min decimal value
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  Min decimal value
   */
  def getDQ_MinDecimalValue: Decimal = _DQMinDecimalValue

  /** Set data quality min decimal value
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Min decimal value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setDQ_MinDecimalValue(value: Decimal, externalCode: String = null): huemul_Columns = {
    if (checkNotCloseDefinition("DQ min decimal")) {
      _DQMinDecimalValue = value
      _DQMinDecimalValueExternalCode = if (externalCode == null) _DQMinDecimalValueExternalCode else externalCode
    }
    this
  }

  /** Set min decimal value data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return        [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def setDQ_MinDecimalValue_Notification(notification: huemulType_DQNotification): huemul_Columns = {
    if (checkNotCloseAndNotNull("DQ min length",notification))
      _DQMinDecimalValueNotification = notification

    this
  }

  /** Get min decimal value data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @return  [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def getDQ_MinDecimalValue_Notification: huemulType_DQNotification = _DQMinDecimalValueNotification

  /** Set data quality min decimal value
   * @since   1.1
   * @group   column_data_quality
   *
   * @param value   Min decimal value
   */
  @deprecated("this method will be removed, instead use setDQ_MinDecimalValue(value: Decimal, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MinDecimalValue(value: Decimal) {
    if (checkNotCloseDefinition("DQ min decimal"))
      _DQMinDecimalValue = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MAX decimal value ----------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _DQMaxDecimalValue: Decimal = _
  private var _DQMaxDecimalValueExternalCode: String = "HUEMUL_DQ_007"
  private var _DQMaxDecimalValueNotification: huemulType_DQNotification = _

  /** Get data quality max decimal value error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  String error code
   */
  def getDQ_MaxDecimalValue_externalCode: String = _DQMaxDecimalValueExternalCode

  /** Get data quality max decimal value
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  Max decimal value
   */
  def getDQ_MaxDecimalValue: Decimal = _DQMaxDecimalValue

  /** Set data quality max decimal value
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Max decimal value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setDQ_MaxDecimalValue(value: Decimal, externalCode: String = null): huemul_Columns = {
    if (checkNotCloseDefinition("DQ max decimal")) {
      _DQMaxDecimalValue = value
      _DQMaxDecimalValueExternalCode = if (externalCode == null) _DQMaxDecimalValueExternalCode else externalCode
    }
    this
  }

  /** Set max decimal data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return              [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def setDQ_MaxDecimalValue_Notification(notification: huemulType_DQNotification): huemul_Columns = {
    if (checkNotCloseAndNotNull("DQ max decimal", notification))
      _DQMaxDecimalValueNotification = notification
    this
  }

  /** Get max decimal data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @return  [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def getDQ_MaxDecimalValue_Notification: huemulType_DQNotification = _DQMaxDecimalValueNotification

  /** Set data quality max decimal value
   * @since   1.1
   * @group   column_data_quality
   *
   * @param value   Max decimal value
   */
  @deprecated("this method will be removed, instead use setDQ_MaxDecimalValue(value: Decimal, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MaxDecimalValue(value: Decimal) {
    if (checkNotCloseDefinition("DQ max decimal"))
      _DQMaxDecimalValue = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MIN DateTime value ---------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _DQMinDateTimeValue: String = _
  private var _DQMinDateTimeValueExternalCode: String = "HUEMUL_DQ_008"
  private var _DQMinDateTimeValueNotification: huemulType_DQNotification = _

  /** Get data quality min date time value error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  String error code
   */
  def getDQ_MinDateTimeValue_externalCode: String = _DQMinDateTimeValueExternalCode

  /** Get data quality min date time value
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  Min date time value
   */
  def getDQ_MinDateTimeValue: String = _DQMinDateTimeValue

  /** Set data quality min date time value
   *
   * Format: YYYY-MM-DD HH:mm:ss or fieldName
   *
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Min date time value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setDQ_MinDateTimeValue(value: String, externalCode: String = null): huemul_Columns = {
    if (checkNotCloseDefinition("DQ min datetime")) {
      _DQMinDateTimeValue = value
      _DQMinDateTimeValueExternalCode = if (externalCode == null) _DQMinDateTimeValueExternalCode  else externalCode
    }
    this
  }

  /** Set min datetime value data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return              [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def setDQ_MinDateTimeValue_Notification(notification: huemulType_DQNotification): huemul_Columns = {
    if (checkNotCloseAndNotNull("DQ min datetime", notification))
      _DQMinDateTimeValueNotification = notification
    this
  }

  /** Get min datetime value data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @return  [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def getDQ_MinDateTimeValue_Notification: huemulType_DQNotification = _DQMinDateTimeValueNotification

  /** Set data quality min date time value
   * @since   1.1
   * @group   column_data_quality
   *
   * @param value   Min date time value
   */
  @deprecated("this method will be removed, instead use setDQ_MinDateTimeValue(value: String, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MinDateTimeValue(value: String) {
    if (checkNotCloseDefinition("DQ min datetime"))
      _DQMinDateTimeValue = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality MAX DateTime value ---------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  private var _DQMaxDateTimeValue: String = _
  private var _DQMaxDateTimeValueExternalCode: String = "HUEMUL_DQ_008"
  private var _DQMaxDateTimeValueNotification: huemulType_DQNotification = _

  /** Get data quality max date time value error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  String error code
   */
  def getDQ_MaxDateTimeValue_externalCode: String = _DQMaxDateTimeValueExternalCode

  /** Get data quality max date time value
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  Max date time value
   */
  def getDQ_MaxDateTimeValue: String = _DQMaxDateTimeValue

  /** Set data quality max date time value
   *
   * Format: YYYY-MM-DD HH:mm:ss or fieldName
   *
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Max date time value
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setDQ_MaxDateTimeValue(value: String, externalCode: String = null): huemul_Columns = {
    if (checkNotCloseDefinition("DQ max datetime")) {
      _DQMaxDateTimeValue = value
      _DQMaxDateTimeValueExternalCode = if (externalCode == null )  _DQMaxDateTimeValueExternalCode else externalCode
    }
    this
  }

  /** Set max datetime value data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return              [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def setDQ_MaxDateTimeValue_Notification(notification: huemulType_DQNotification): huemul_Columns = {
    if (checkNotCloseAndNotNull("DQ max datetime", notification))
      _DQMaxDateTimeValueNotification = notification
    this
  }

  /** Get min datetime value data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @return  [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def getDQ_MaxDateTimeValue_Notification: huemulType_DQNotification = _DQMaxDateTimeValueNotification

  /** Set data quality max date time value
   * @since   1.1
   * @group   column_data_quality
   *
   * @param value   Max date time value
   */
  @deprecated("this method will be removed, instead use setDQ_MaxDateTimeValue(value: String, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MaxDateTimeValue(value: String) {
    if (checkNotCloseDefinition("DQ max datetime"))
      _DQMaxDateTimeValue = value
  }


  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality Regular Expression value ---------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // ToDo: Validate Regular Expression
  private var _DQRegExp: String = _
  private var _DQRegExpExternalCode: String = "HUEMUL_DQ_005"
  private var _DQRegExpNotification: huemulType_DQNotification = _

  /** Get data quality regular expression error code
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  String error code
   */
  def getDQ_RegExpression_externalCode: String = _DQRegExpExternalCode
  @deprecated("this method will be removed, instead use setDQ_RegExpression(value: String, externalCode: String = null): huemul_Columns", "3.0")
  def getDQ_RegExp_externalCode: String = _DQRegExpExternalCode //depreciated to meet projects name pattern (getDQ_Expression....)

  /** Get data quality regular expression
   * @since   1.1
   * @group   column_data_quality
   *
   * @return  Max date time value
   */
  def getDQ_RegExpression: String = _DQRegExp
  @deprecated("this method will be removed, instead use setDQ_RegExpression(value: String, externalCode: String = null): huemul_Columns", "3.0")
  def getDQ_RegExp: String = _DQRegExp  //depreciated to meet projects name pattern (getDQ_Expression....)

  /** Set data quality regular expression
   * @since   2.1
   * @group   column_data_quality
   *
   * @param value         Regular expression statement
   * @param externalCode  Externa error code
   * @return              [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setDQ_RegExpression(value: String, externalCode: String = null): huemul_Columns = {
    if (checkNotCloseDefinition("DQ RegExpression")) {
      _DQRegExp = value
      _DQRegExpExternalCode = if (externalCode == null) _DQRegExpExternalCode  else externalCode
    }
    this
  }


  /** Set max datetime value data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param notification  ERROR, WARNING, WARNING_EXCLUDE
   * @return              [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setDQ_RegExpression_Notification(notification: huemulType_DQNotification): huemul_Columns = {
    if (checkNotCloseAndNotNull("DQ RegExpression", notification))
      _DQRegExpNotification = notification
    this
  }

  /** Get min datetime value data quality rule notification
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @return  [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def getDQ_RegExpression_Notification: huemulType_DQNotification = _DQRegExpNotification

  /** Set data quality regular expression
   * @since   1.1
   * @group   column_data_quality
   *
   * @param value   Regular expression statement
   */
  @deprecated("this method will be removed, instead use setDQ_RegExpression(value: String, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_RegExp(value: String, dq_externalCode: String = null) {
    if (checkNotCloseDefinition("DQ RegExpression")) {
      _DQRegExp = value
      _DQRegExpExternalCode = dq_externalCode
    }
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Column data quality notification ---------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // Column Data Rule Notification Level ERROR(default), WARNING or WARNING_EXCLUDE for all column data rules
  private var _DQNotification: huemulType_DQNotification = huemulType_DQNotification.ERROR

  /** Set de Error Notification Level for the column data quality for all column data rules
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @param value   Notification Level ERROR (default), WARNING or WARNING_EXCLUDE
   * @return        [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def setDQ_Notification(value: huemulType_DQNotification ): huemul_Columns = {
    _DQNotification = value
    this
  }

  /** Get de Error Notification Level for the column data quality for all column data rules
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @group   column_data_quality
   *
   * @return  [[com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification]]
   */
  def getDQ_NotificationX: huemulType_DQNotification = _DQNotification

  // ******************************************************************************************************************
  // MDM Properties
  // ******************************************************************************************************************

  // ------------------------------------------------------------------------------------------------------------------
  // MDM Enable old value full trace ----------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // save all old values trace in other table (default disable)
  private var _MDMEnableOldValueFullTrace: Boolean = false

  /** Check if MDM old value full trace is enabled
   * @since   1.1
   * @group   column_mdm
   *
   * @return  Boolean
   */
  def getMDM_EnableOldValue_FullTrace: Boolean = _MDMEnableOldValueFullTrace

  /** Set MDM old value full trace
   *
   * Save all old values trace in other table (default false)
   *
   * @since   2.1
   * @group   column_mdm
   *
   * @return  [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setMDM_EnableOldValue_FullTrace(): huemul_Columns = {
    if (checkNotCloseDefinition("MDM EnableOldValue FullTrace"))
      _MDMEnableOldValueFullTrace = true
    this
  }

  /** Set MDM old value full trace.
   *
   * Save all old values trace in other table
   *
   * @since   1.4
   * @group   column_mdm
   *
   * @param  value  true/false
   */
  @deprecated("this method will be removed, instead use setMDM_EnableOldValue_FullTrace(): huemul_Columns", "3.0")
  def setMDM_EnableOldValue_FullTrace(value: Boolean) {
    if (checkNotCloseAndNotNull("MDM EnableOldValue FullTrace", value))
      _MDMEnableOldValueFullTrace = value
  }


  // ------------------------------------------------------------------------------------------------------------------
  // MDM Enable old value ---------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // save old value when new value arrive
  private var _MDMEnableOldValue: Boolean = false

  /** Check if MDM old value is enabled
   * @since   1.4
   * @group   column_mdm
   *
   * @return  Boolean
   */
  def getMDM_EnableOldValue: Boolean = _MDMEnableOldValue


  /** Set MDM old value full trace.
   *
   * Save old value when new value arrive (default false)
   *
   * @since   2.1
   * @group   column_mdm
   *
   * @return  [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setMDM_EnableOldValue(): huemul_Columns = {
    if (checkNotCloseDefinition("MDM EnableOldValue"))
      _MDMEnableOldValue = true
    this
  }

  /** Set MDM old value full trace.
   *
   * Save old value when new value arrive (default false)
   *
   * @since   2.1
   * @group   column_mdm
   *
   * @param  value  true/false
   */
  @deprecated("this method will be removed, instead use setMDM_EnableOldValue(): huemul_Columns", "3.0")
  def setMDM_EnableOldValue(value: Boolean) {
    if (checkNotCloseAndNotNull("MDM EnableOldValue", value))
      _MDMEnableOldValue = value
  }


  // ------------------------------------------------------------------------------------------------------------------
  // MDM Enable old value add datetime column -------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // create a new field with datetime log for change value
  private var _MDMEnableDTLog: Boolean = false

  /** Check if MDM old value Datetime log is enabled
   * @since   1.4
   * @group   column_mdm
   *
   * @return  Boolean
   */
  def getMDM_EnableDTLog: Boolean = _MDMEnableDTLog

  /** Enable MDM old value datetime log on additional column/field.
   *
   * create a new field with datetime log for change value (default false)
   *
   * @since   2.1
   * @group   column_mdm
   *
   * @return  [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setMDM_EnableDTLog(): huemul_Columns = {
    if (checkNotCloseDefinition("MDM EnableDTLog"))
      _MDMEnableDTLog = true
    this
  }

  /** Enable MDM old value datetime log on additional column/field.
   *
   * create a new field with datetime log for change value (default false)
   *
   * @since  2.1
   * @group  column_mdm
   *
   * @param  value  true/false
   */
  @deprecated("this method will be removed, instead use setMDM_EnableDTLog(): huemul_Columns", "3.0")
  def setMDM_EnableDTLog(value: Boolean) {
    if (checkNotCloseAndNotNull("MDM EnableDTLog", value))
      _MDMEnableDTLog = value
  }


  // ------------------------------------------------------------------------------------------------------------------
  // MDM Enable old value add datetime column -------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------
  // Create a new field with Process Log for change value
  private var _MDMEnableProcessLog: Boolean = false

  /** Check if MDM old value process name log is enabled
   * @since   1.4
   * @group   column_mdm
   *
   * @return  Boolean
   */
  def getMDM_EnableProcessLog: Boolean = _MDMEnableProcessLog

  /** Enable MDM old value datetime log on additional column/field.
   *
   * Create a new field with Process Name, who did the change value (default false)
   *
   * @since   2.1
   * @group   column_mdm
   *
   * @return  [[com.huemulsolutions.bigdata.tables.huemul_Columns]]
   */
  def setMDM_EnableProcessLog(): huemul_Columns = {
    if (checkNotCloseDefinition("MDM EnableProcessLog"))
      _MDMEnableProcessLog = true
    this
  }

  /** Enable MDM old value datetime log on additional column/field.
   *
   * Create a new field with Process Name, who did the change value (default false)
   *
   * @since   2.1
   * @group   column_mdm
   *
   * @param  value  true/false
   */
  @deprecated("this method will be removed, instead use setMDM_EnableProcessLog(): huemul_Columns", "3.0")
  def setMDM_EnableProcessLog(value: Boolean) {
    if (checkNotCloseAndNotNull("MDM EnableProcessLog", value))
      _MDMEnableProcessLog = value
  }

  // ------------------------------------------------------------------------------------------------------------------
  // User mapping attributes
  // ------------------------------------------------------------------------------------------------------------------
  private var _MappedName: String = _
  private var _MyName: String = _
  private var _ReplaceValueOnUpdate: Boolean = false
  private var _SQLForUpdate: String = _
  private var _SQLForInsert: String = _

  @deprecated("this method will be removed, instead use setMapping(): huemul_Columns", "3.0")
  def Set_MyName(name: String) {
    setMyName(name)
  }

  /** Set Column Name
   *
   * @param name  Column Name
   */
  def setMyName(name: String) {
    this._MyName = name
  }

  /**
   *
   * @param tableStorage  [[com.huemulsolutions.bigdata.tables.huemulType_StorageType]] Identify storage type PARQUET, etc.
   * @return
   */
  def getMyName(tableStorage: huemulType_StorageType): String = {
    if (this._MyName == null || this._MyName == "")
      throw new Exception(s"Huemul ERROR: MUST call 'ApplyTableDefinition' in table definition, (field description: ${this.Description} )")

    getCaseType(tableStorage,this._MyName)
  }

  /**
   *
   * @param tableStorage  [[com.huemulsolutions.bigdata.tables.huemulType_StorageType]] Identify storage type PARQUET, etc.
   * @return
   */
  @deprecated("this method will be removed, instead use setMapping(): huemul_Columns", "3.0")
  def get_MyName(tableStorage: huemulType_StorageType): String = getMyName(tableStorage)

  /** Convert column name caso for compatibility between table storage's
   *
   * ToDo: replicated en huemul_BigDataGovernance -> evaluate to mive sperata Object class (static class)
   *
   * @param tableStorage  [[com.huemulsolutions.bigdata.tables.huemulType_StorageType]] Identify storage type PARQUET, etc.
   * @param value         Data to be convert the case
   * @return              String
   */
  def getCaseType(tableStorage: huemulType_StorageType, value: String): String =
    if (tableStorage == huemulType_StorageType.AVRO) value.toLowerCase() else value

  /** Get the mapping name
   *
   * @return String
   */
  def getMappedName: String = this._MappedName

  /** Get the mapping name
   *
   * @return String
   */
  @deprecated("this method will be removed, instead use setMapping(): huemul_Columns", "3.0")
  def get_MappedName(): String = getMappedName

  /**
   *
   * @return
   */
  def getReplaceValueOnUpdate: Boolean = this._ReplaceValueOnUpdate

  /**
   *
   * @return
   */
  @deprecated("this method will be removed, instead use setMapping(): huemul_Columns", "3.0")
  def get_ReplaceValueOnUpdate(): Boolean = getReplaceValueOnUpdate

  /**
   *
   * @return
   */
  def getSQLForUpdate: String = this._SQLForUpdate

  /**
   *
   * @return
   */
  @deprecated("this method will be removed, instead use setMapping(): huemul_Columns", "3.0")
  def get_SQLForUpdate(): String = getSQLForUpdate

  /**
   *
   * @return
   */
  def getSQLForInsert: String = this._SQLForInsert

  /**
   *
   * @return
   */
  @deprecated("this method will be removed, instead use setMapping(): huemul_Columns", "3.0")
  def get_SQLForInsert(): String = getSQLForInsert

  /** Set mapping DataFrame fields to Table Fields for insert and update.
   *
   * For SQLForUpdate and SQLForInsert, in both cases, don't include "as Namefield" instead "old" is a reference to
   * the table and "new" is reference to your DataFrame
   *
   * @param param_name            fields name in DataFrame
   * @param ReplaceValueOnUpdate  true (default) for replace values in table
   * @param SQLForUpdate          example: cast(case when old.[field] > new.[field] then old.[field] else new.[field] end as Integer)
   * @param SQLForInsert          example: cast(case when old.[field] > new.[field] then old.[field] else new.[field] end as Integer)
   */
  def setMapping(param_name: String, ReplaceValueOnUpdate: Boolean = true, SQLForUpdate: String = null, SQLForInsert: String = null) {
    this._MappedName = param_name
    this._ReplaceValueOnUpdate = ReplaceValueOnUpdate
    this._SQLForUpdate = SQLForUpdate
    this._SQLForInsert = SQLForInsert
  }

  /** Set mapping DataFrame fields to Table Fields for insert and update.
   *
   * For SQLForUpdate and SQLForInsert, in both cases, don't include "as Namefield" instead "old" is a reference to
   * the table and "new" is reference to your DataFrame
   *
   * @param param_name            fields name in DataFrame
   * @param ReplaceValueOnUpdate  true (default) for replace values in table
   * @param SQLForUpdate          example: cast(case when old.[field] > new.[field] then old.[field] else new.[field] end as Integer)
   * @param SQLForInsert          example: cast(case when old.[field] > new.[field] then old.[field] else new.[field] end as Integer)
   */
  @deprecated("this method will be removed, instead use setMapping(): huemul_Columns", "3.0")
  def SetMapping(param_name: String, ReplaceValueOnUpdate: Boolean = true, SQLForUpdate: String = null, SQLForInsert: String = null) {
    setMapping(param_name, ReplaceValueOnUpdate, SQLForUpdate, SQLForInsert)
  }

  /** Get DataType to be deployed
   *
   * Return data type with AVRO and spark < 2.4 compatibility
   *
   * @since 2.5
   *
   * @param bigDataProvider   [[com.huemulsolutions.bigdata.common.huemulType_bigDataProvider]] Identify Data Provider like Databrick
   * @param storageType       [[com.huemulsolutions.bigdata.tables.huemulType_StorageType]] Identify storage type PARQUET
   *                          , ORC, AVRO, HBASE, DELTA
   * @return                  DataType
   */
  def getDataTypeDeploy(bigDataProvider: huemulType_bigDataProvider, storageType: huemulType_StorageType ): DataType = {
    var result = this.DataType
    if (storageType == huemulType_StorageType.AVRO
      && bigDataProvider != huemulType_bigDataProvider.databricks
      && ( result.sql.toUpperCase().contains("DATE") || result.sql.toUpperCase().contains("TIMESTAMP"))
    ) {
      result = StringType
    } else if (storageType == huemulType_StorageType.AVRO
      && bigDataProvider == huemulType_bigDataProvider.databricks
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
  private var _hive_df: String = "default"
  private var _hive_col: String = _

  /** Define family and column name on HBase Tables
   *
   * @param df
   * @param col
   * @return
   */
  def setHBaseCatalogMapping(df: String, col: String = null): huemul_Columns = {
    _hive_df = df
    _hive_col = col
    this
  }

  /**
   *
   * @return
   */
  def getHBaseCatalogFamily(): String = _hive_df

  /**
   *
   * @return
   */
  def getHBaseCatalogColumn(): String = if (_hive_col == null) getMyName(huemulType_StorageType.HBASE) else _hive_col

  /** Get hbase data type
   *
   * @return  String data type
   */
  def getHBaseDataType(): String = {
    if (DataType == DataTypes.StringType) "string"
    else if (DataType == DataTypes.IntegerType) "int"
    else if (DataType == DataTypes.ShortType) "smallint"    //TODO: Repetido ShortType
    else if (DataType == DataTypes.BooleanType) "boolean"
    else if (DataType == DataTypes.DoubleType) "double"
    else if (DataType == DataTypes.FloatType) "float"
    else if (DataType == DataTypes.LongType) "bigint"
    else if (DataType == DataTypes.ShortType) "tinyint"     //TODO: Repetido ShortType
    else if (DataType == DataTypes.BinaryType) "binary"
    else "string"
  }

}
