package samples

import org.junit._
import Assert._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.control.huemulType_Frequency


@Test
class AppTest {
    val args: Array[String] = new Array[String](1)
    args(0) = "Environment=production,RegisterInControl=false,TestPlanMode=true"
      
    val huemulBigDataGov = new huemul_BigDataGovernance("Pruebas Inicialización de Clases",args,globalSettings.Global)
    val Control = new huemul_Control(huemulBigDataGov,null, huemulType_Frequency.ANY_MOMENT)
      
    /*TEST PARA HUEMUL_LIBRARY*/
    
    @Test
    def test_HasName_OK() = assertTrue(huemulBigDataGov.HasName("si tiene"))
    
    @Test
    def test_HasName_OK_Espacios() = assertTrue(huemulBigDataGov.HasName("  "))
   
    @Test
    def test_HasName_OK_Nulo() = assertFalse(huemulBigDataGov.HasName(null))
    
    @Test
    def test_HasName_OK_Vacio() = assertFalse(huemulBigDataGov.HasName(""))
    
    @Test
    def test_GetYear() = assertTrue(huemulBigDataGov.getYear(huemulBigDataGov.setDate("2018-12-31")) == 2018) 
    
    @Test
    def test_GetMonth() = assertTrue(huemulBigDataGov.getMonth(huemulBigDataGov.setDate("2018-12-31")) == 12) 
     
    @Test
    def test_GetDay() = assertTrue(huemulBigDataGov.getDay(huemulBigDataGov.setDate("2018-12-31")) == 31) 
    
    /*TEST PARA HUEMUL_TABLE, HUEMUL_COLUMN*/
    var TestTable: tbl_demo_test = null
    try {
       TestTable = new tbl_demo_test(huemulBigDataGov, Control)  
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    
    @Test
    def test_TableIsOK() = assertFalse(TestTable.Error_isError) 
    
    /*TEST PARA HUEMUL_TABLE, HUEMUL_COLUMN, huemul_tablerelationship*/
    var TestTable_padre: tbl_demo_test_padre = null
    try {
       TestTable_padre = new tbl_demo_test_padre(huemulBigDataGov, Control)  
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    
    @Test
    def test_TablePadreIsOK() = assertFalse(TestTable_padre.Error_isError) 


}
