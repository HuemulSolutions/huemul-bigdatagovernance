package com.huemulsolutions.bigdata.common

class huemul_ExternalDB() extends Serializable {
    var Using_SPARK: huemul_ExternalDBType = new huemul_ExternalDBType().setActive(true).setActiveForHBASE(true)
    var Using_HIVE: huemul_ExternalDBType = new huemul_ExternalDBType()
}
