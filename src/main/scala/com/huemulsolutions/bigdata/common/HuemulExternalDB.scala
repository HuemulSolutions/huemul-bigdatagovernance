package com.huemulsolutions.bigdata.common

class HuemulExternalDB() extends Serializable {
    var Using_SPARK: HuemulExternalDBType = new HuemulExternalDBType().setActive(true).setActiveForHBASE(false)
    var Using_HIVE: HuemulExternalDBType = new HuemulExternalDBType()
    var Using_HWC: HuemulExternalDBType = new HuemulExternalDBType()
    
    
}
