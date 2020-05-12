# `Desafío Teórico` - Readme

# Consigna
## Procesos, hilos y corrutinas


- Un caso en el que usarías procesos para resolver un problema y por qué.
Resp: 
Caso: Hipermercado Makro, cuenta con varias sucursales y en cada una de ellas mas de 20 cajas. Cada tienda, genera mas de 5mil ventas diarias y al final del dia se requiere generar un reporte de ventas por cajas, tienda y general, Asi como tambien el calculo mensual e indicadores por tienda. Actualmente ellos cuentan con procesos realidos en proc, invocados en shell y todos dependientes. 
Para este escenario usaria procesos, debido a que permite paralelizar las tareas de calculos y el tiempo de proceso se reduceria drasticamente. Multiprocess, aprovecha el beneficio de equipos con varios nucleos, está diseñado explícitamente para compartir grandes cargas de trabajo de CPU en varias CPU.


- Un caso en el que usarías threads para resolver un problema y por qué.
Resp:
Caso: Se requiere obtener informacion de diferentes sites, para luego procesar esos datos. 
Utilizaria hilos o threads en la obtencion de esa informacion, como es una operacion de tipo E/S, y no de tipo CPU. con el paralelizaria esta tarea y permitiria compartir recursos entre si. Debido a que los hilos en ejecución de un proceso comparten el mismo espacio de datos que el hilo principal y pueden, por tanto, tener acceso a la misma información o comunicarse entre sí más fácilmente que si estuvieran en procesos separados y suele requerir menos recursos de memoria que ejecutar lo equivalente en procesos separados.


- Un caso en el que usarías corrutinas para resolver un problema y por qué.
Caso: Se requiere cargar grandes datos en memoria, la cual se necesita procesar toda esa informacion. 
Resp: Utilizaria corutines para cargar por secciones con el fin de procesar en paralelo. Y la informacion la cargaria en mnemoria solo cuando la misma es llanmda
La  corutina proporcionan concurrencia y puede combinarse con , no es costoso a nivell de memoria, el costo de iniciar una llamada a una funcion usa menos de 1k de memoria. son un tipo procedimientos o subrutinas de las que se puede salir por diversos puntos para posteriormente volver y reiniciar la ejecución desde el último punto de retorno.


# Optimización de recursos del sistema operativo
Si tuvieras 1.000.000 de elementos y tuvieras que consultar para cada uno de ellos información en una API HTTP. 
¿Cómo lo harías? Explicar.

Realizaria una corutine para agruparlas en bloque de N, luego utillizaria threads para para que cada uno de ellos procese el sub-bloque de esas peticiones y el response lo almacenaria en una lista, que debido a que comparte recursos en memoria no es necesario el uo de semaphore.
