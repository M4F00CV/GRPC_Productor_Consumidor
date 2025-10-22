# GRPC_Productor_Consumidor
uso de GRPC para el modelo productor-consumidor

Este proyecto implementa un sistema de procesamiento de tareas distribuido utilizando gRPC para la comunicación de alto rendimiento entre un servidor central y múltiples workers (clientes).

El sistema se basa en una arquitectura de productor-consumidor. El servidor actúa como productor, generando conjuntos de datos únicos (vectores numéricos aleatorios) que se encolan para ser procesados.

Los workers (clientes) se conectan concurrentemente al servidor para consumir estas tareas. Cada worker aplica una operación de cómputo definida (como una función matemática) sobre los datos recibidos y devuelve el resultado al servidor.

El servidor es responsable de dos tareas principales:

Agregación de Resultados: Recopila todos los resultados devueltos por los workers y calcula una métrica global (por ejemplo, la suma total).

Seguimiento de Rendimiento: Mantiene un registro del historial de contribuciones para identificar a los workers más activos del sistema.

El sistema está diseñado para gestionar un alto volumen de transacciones y opera hasta alcanzar un umbral predefinido de un millón de resultados procesados.
