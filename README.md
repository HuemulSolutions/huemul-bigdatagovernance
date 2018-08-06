# HuemulBigData
Esta librería permite simplificar y optimizar el trabajo de DataEngineering para la creación
de cargas de información en ambientes complejos de datos.

algunas características:

- Permite hacer la definición de tablas en una clase de scala
    - las tablas son creadas en parquet (o el formato que el usuario escoja)
    - las tablas también son automáticamente creadas en Hive, como tabla externa
    - si usa Impala, se crea automáticamente la referencia
    
- En la clase en scala, se definen los campos, con los criterios de DataQuality en forma sencilla.
 	- máximos y mínimos de largo, fechas y números
 	- Validación de Primary Key
 	- Validación de Foreing Key (solo al momento de crear los datos)
 	- Posibilidad de agregar DataQuality fácilmente, definiendo umbrales de éxito
 	
 - Control de procesos
 	- Se tiene trazabilidad de archivos RAW --> Tablas
 