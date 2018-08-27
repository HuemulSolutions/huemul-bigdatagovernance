# HuemulBigData (Versión 1.0 estará disponible durante el mes de septiembre del 2018)
Esta librer�a permite simplificar y optimizar el trabajo de DataEngineering para la creaci�n
de cargas de informaci�n en ambientes complejos de datos.

algunas caracter�sticas:

- Permite hacer la definici�n de tablas en una clase de scala
    - las tablas son creadas en parquet (o el formato que el usuario escoja)
    - las tablas tambi�n son autom�ticamente creadas en Hive, como tabla externa
    - si usa Impala, se crea autom�ticamente la referencia
    
- En la clase en scala, se definen los campos, con los criterios de DataQuality en forma sencilla.
 	- m�ximos y m�nimos de largo, fechas y n�meros
 	- Validaci�n de Primary Key
 	- Validaci�n de Foreing Key (solo al momento de crear los datos)
 	- Posibilidad de agregar DataQuality f�cilmente, definiendo umbrales de �xito
 	
 - Control de procesos
 	- Se tiene trazabilidad de archivos RAW --> Tablas
 
