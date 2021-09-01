package samples

import org.junit._
import Assert._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.control.HuemulTypeFrequency



@Test
class AppTest {
    val args: Array[String] = new Array[String](1)
    args(0) = "Environment=production,RegisterInControl=false,TestPlanMode=true"
      
    val huemulBigDataGov = new HuemulBigDataGovernance("Pruebas Inicialización de Clases",args,globalSettings.Global)
    val Control = new HuemulControl(huemulBigDataGov,null, HuemulTypeFrequency.ANY_MOMENT)
      
    /*TEST PARA HUEMUL_LIBRARY*/
    
    
    @Test
    def test_DifFechas_OK(): Unit = assertTrue(diffechas())
    
    def diffechas(): Boolean = {
     
      
      /*
      val fechaUno = huemulBigDataGov.setDateTime(2018, 10, 29, 15, 20, 5)
      val fechados = huemulBigDataGov.setDateTime(2018, 10, 1, 10, 30, 10)
      
      
      val dif = fechaUno.getTimeInMillis - fechados.getTimeInMillis
      
      println("TEST DE FECHAS")
      println(huemulBigDataGov.dateTimeFormat.format(fechaUno.getTime))
      println(huemulBigDataGov.dateTimeFormat.format(fechados.getTime))
      println(dif)
      
      var calc = dif / 1000
      val sec = calc % 60
      calc /= 60
      val min = calc % 60
      calc /= 60
      val hour = calc % 24
      calc /= 24
      val day = calc
      
      println (s"$day dias, $hour horas, $min minutos, $sec segundos")
      
      //fechados.setTimeInMillis(dif)
      //println(huemulBigDataGov.dateTimeFormat.format(fechados.getTime))
      println("TEST DE FECHAS")
      *  * 
      */
      
       true
     
    }
    
    //println(Control.ReplaceSQLStringNulls("1234", 3))
    
    @Test
    def test_replaceSQLStringNulls_OK(): Unit = assertTrue(Control.ReplaceSQLStringNulls("1234", 3) == "'123'" )
    
    @Test
    def test_replaceSQLStringNulls_null_OK(): Unit = assertTrue(Control.ReplaceSQLStringNulls("1234", null) == "'1234'" )
    
    @Test
    def test_replaceSQLStringNulls_ISnull_OK(): Unit = assertTrue(Control.ReplaceSQLStringNulls(null, null) == "null" )
    
    
    @Test
    def test_HasName_OK(): Unit = assertTrue(huemulBigDataGov.HasName("si tiene"))
    
    @Test
    def test_HasName_OK_Espacios(): Unit = assertTrue(huemulBigDataGov.HasName("  "))
   
    @Test
    def test_HasName_OK_Nulo(): Unit = assertFalse(huemulBigDataGov.HasName(null))
    
    @Test
    def test_HasName_OK_Vacio(): Unit = assertFalse(huemulBigDataGov.HasName(""))
    
    @Test
    def test_GetYear(): Unit = assertTrue(huemulBigDataGov.getYear(huemulBigDataGov.setDate("2018-12-31")) == 2018)
    
    @Test
    def test_GetMonth(): Unit = assertTrue(huemulBigDataGov.getMonth(huemulBigDataGov.setDate("2018-12-31")) == 12)
     
    @Test
    def test_GetDay(): Unit = assertTrue(huemulBigDataGov.getDay(huemulBigDataGov.setDate("2018-12-31")) == 31)
    
    /*TEST PARA HUEMUL_TABLE, HUEMUL_COLUMN*/
    var TestTable: tbl_demo_test = _
    try {
       TestTable = new tbl_demo_test(huemulBigDataGov, Control)  
       println("***************************************TestTable.GetOrderByColumn()")
       println(TestTable.getOrderByColumn)
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    
    @Test
    def test_TableIsOK(): Unit = assertFalse(TestTable.Error_isError)
    
    /*TEST PARA HUEMUL_TABLE, HUEMUL_COLUMN, huemul_tablerelationship*/
    var TestTable_padre: tbl_demo_test_padre = _
    try {
       TestTable_padre = new tbl_demo_test_padre(huemulBigDataGov, Control)  
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    
    @Test
    def test_TablePadreIsOK(): Unit = assertFalse(TestTable_padre.Error_isError)


}
