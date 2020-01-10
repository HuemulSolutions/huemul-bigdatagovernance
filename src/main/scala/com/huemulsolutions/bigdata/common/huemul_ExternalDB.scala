package com.huemulsolutions.bigdata.common

class huemul_ExternalDB() extends Serializable {
    var externalTableUsing_SPARK: huemul_ExternalDBType = new huemul_ExternalDBType().setActive(true).setActiveForHBASE(true)
    var externalTableUsing_HIVE: huemul_ExternalDBType = new huemul_ExternalDBType()
}
