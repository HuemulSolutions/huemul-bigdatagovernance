package com.huemulsolutions.bigdata.common

class HuemulExternalDb() extends Serializable {
    var usingSpark: HuemulExternalDbType = new HuemulExternalDbType().setActive(true).setActiveForHBASE(false)
    var usingHive: HuemulExternalDbType = new HuemulExternalDbType()
    var usingHwc: HuemulExternalDbType = new HuemulExternalDbType()
    
    
}
