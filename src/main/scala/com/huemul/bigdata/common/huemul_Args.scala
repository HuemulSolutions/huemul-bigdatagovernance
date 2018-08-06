package com.huemul.bigdata.common

import java.util.HashMap;

/**
 * huemul_Args: get all arguments from console
 */
class huemul_Args() extends Serializable {
  var argumentos: HashMap[String, String] = new HashMap[String, String]();
  
  /**
  * Setea variables definidas como parametros al ejecutar el programa, llevando a lower case tanto la llave
  * como el valor 
  * @param args Array de String que contiene los argumentos ingresados al ejecutar el programa  
  */
  def setArgs(args: Array[String]) = {
    // caso existencia de parametros
    argumentos = new HashMap[String, String]();
    if (args == null) {
      val a = 1  
    }
    else if( args.length > 0 ){
      args.foreach { x => 
          val argumentos_divididos = x.split(',')
                                            .map( x => x.split('=') )              
          val argumentos_completos = argumentos_divididos.filter( x => x.length == 2)    // parametros con clave=valor
          val argumentos_incompletos = argumentos_divididos.filter( x => x.length != 2)  // parametros con valor vacio
                            
          argumentos_completos.foreach { x => 
            argumentos.put(x(0).toLowerCase(), x(1).toLowerCase())
            //println(s"${x(0).toLowerCase()} = ${x(1).toLowerCase()}")
          };  
          argumentos_incompletos.foreach { x => 
            argumentos.put(x(0).toLowerCase(), "")
            //println(s"${x(0).toLowerCase()} = null")
          };
      }
    } 
    
     
  }
  
  /***
   * GetValue(Key: String, DefaultValue: String): String
   * Get params value, if does't exist, return null
   */
  def GetValue(Key: String, DefaultValue: String): String = {
    val KeyLower = Key.toLowerCase()
    var Value: String = DefaultValue

    if (argumentos.containsKey(KeyLower)) Value = argumentos.get(KeyLower).toLowerCase()
    
    return Value
  }
  
  /***
   * GetValue(Key: String, DefaultValue: String, ErrorMessageIfNotExist: String)
   * Get params value, if does't exist, raiseError
   */
  def GetValue(Key: String, DefaultValue: String, ErrorMessageIfNotExist: String): String = {
    var Value: String = GetValue(Key, null)
    if (Value == null){
      sys.error(ErrorMessageIfNotExist)
    }
    
    return Value
  }
}