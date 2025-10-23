import grpc
import random
import uuid
import threading
import queue
from collections import Counter
import time
import os
import sys

# Añadir el directorio raíz del proyecto al path de Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from publisher_suscriptor.proto import publisher_pb2
from publisher_suscriptor.proto import publisher_pb2_grpc

# Límite de 1 millón de resultados
OPERACIONES_MAXIMAS= 110

# Eliminamos la variable global 'ELECCION_COLAS'

class publicador(publisher_pb2_grpc.publicadorServicer):
    
    # --- MODIFICADO: __init__ ---
    def __init__(self, modo_seleccion: str): 
        # Almacenamos el modo de selección del servidor
        self.modo_seleccion = modo_seleccion
        print(f"✅ Modo de selección de cola del servidor: {self.modo_seleccion}")

        # Nombres de las colas para el informe
        self.nombres_colas = {1: "principal", 2: "secundaria", 3: "terciaria"}

        # Las 3 colas de tareas
        self.cola_1 = queue.Queue(maxsize=10000) # principal
        self.cola_2 = queue.Queue(maxsize=10000) # secundaria
        self.cola_3 = queue.Queue(maxsize=10000) # terciaria
        
        # --- Variables compartidas (sin cambios) ---
        self.vectores_unicos = set() 
        self.operaciones_contador = 0             
        self.suma_total = 0                 
        self.contador_tareas_cliente = Counter()
        
        # --- NUEVO: Estado para rastrear suscripciones ---
        # Guardará {'client_id': 'una_cola'} o {'client_id': 'dos_colas'}
        self.suscripciones_cliente = {} 
        
        # --- Herramientas de Concurrencia (sin cambios) ---
        self.state_lock = threading.Lock()   
        self.stop_event = threading.Event()  
        self.server_instance = None          
        
        # --- Hilo Generador de Tareas (sin cambios) ---
        self.generator_thread = threading.Thread(target=self._loop_interno_generador, daemon=True)
        self.generator_thread.start()
        print("✅ Hilo generador de tareas iniciado.")
    
    # --- MODIFICADO: Lógica de selección de colas del CLIENTE ---
    # Renombrado y lógica implementada
    def _elegir_una_cola_aleatoria(self):
        """Implementa la lógica del cliente para 'una lista'. 
           Elige una de las 3 colas al azar."""
        return random.randint(1, 3)

    # Renombrado y lógica implementada
    def _elegir_de_dos_colas_aleatorias(self):
        """Implementa la lógica del cliente para 'dos listas'.
           Elige 2 colas diferentes y devuelve una de ellas al azar."""
        # Elige 2 colas únicas de la lista [1, 2, 3]
        cola1, cola2 = random.sample([1, 2, 3], 2)
        # Devuelve una de esas dos al azar para esta petición
        return random.choice([cola1, cola2])
        
    def set_server_instance(self, server):
        self.server_instance = server

    # --- NUEVA FUNCIÓN: Lógica de selección de colas del SERVIDOR ---
    def _seleccionar_cola_servidor(self, n1, n2, n3):
        """
        Implementa los 3 modos de selección del servidor
        basado en el modo_seleccion establecido en __init__.
        """
        if self.modo_seleccion == 'aleatorio':
            # 33% de probabilidad para cada una
            return random.randint(1, 3)
        
        elif self.modo_seleccion == 'ponderado':
            # 50% cola 1, 30% cola 2, 20% cola 3
            colas = [1, 2, 3]
            pesos = [0.5, 0.3, 0.2]
            return random.choices(colas, weights=pesos, k=1)[0]
        
        elif self.modo_seleccion == 'condicional':
            # Contar números pares
            pares = (n1 % 2 == 0) + (n2 % 2 == 0) + (n3 % 2 == 0)
            impares = 3 - pares
            
            if pares == 2:
                return 1 # dos números pares -> cola principal
            elif impares == 2:
                return 2 # dos números impares -> cola secundaria
            else: 
                # tres pares (pares==3) o tres impares (pares==0)
                return 3 # cola terciaria
        else:
            # Fallback por si acaso
            return 1 

    # --- MODIFICADO: Generador de Tareas ---
    def _loop_interno_generador(self):
        """
        Bucle ejecutado por el hilo generador.
        Usa la nueva lógica de _seleccionar_cola_servidor.
        """
        while not self.stop_event.is_set():
            try:
                # 1. Generar 3 números
                n1 = random.randint(1, 1000)
                n2 = random.randint(1, 1000)
                n3 = random.randint(1, 1000)
                
                # 2. Crear un "vector" único
                task_tuple = tuple(sorted((n1, n2, n3)))

                # 3. Verificar unicidad
                if task_tuple not in self.vectores_unicos:
                    self.vectores_unicos.add(task_tuple)
                    
                    # 4. Crear el mensaje de la tarea
                    task_id = str(uuid.uuid4())
                    task_msg = publisher_pb2.envio_datos(
                        rastreador=task_id,
                        num1=task_tuple[0],
                        num2=task_tuple[1],
                        num3=task_tuple[2]
                    )
                    
                    # 5. MODIFICADO: Seleccionar cola según la lógica del servidor
                    cola_destino = self._seleccionar_cola_servidor(n1, n2, n3)
                    
                    if cola_destino == 1:
                        self.cola_1.put(task_msg, timeout=0.5)
                    elif cola_destino == 2:
                        self.cola_2.put(task_msg, timeout=0.5)
                    else: # cola_destino == 3
                        self.cola_3.put(task_msg, timeout=0.5)
                
            except queue.Full:
                time.sleep(0.1)
            except Exception:
                pass
        
        print("🛑 Hilo generador de tareas detenido.")

    # --- MODIFICADO: RPC PedirDatos ---
    def PedirDatos(self, request, context):
        """
        RPC 1: Un cliente pide una tarea.
        MODIFICADO: Asigna colas específicas al cliente en la primera conexión
        y las almacena en self.suscripciones_cliente.
        """
        
        # --- NUEVA LÓGICA DE SUSCRIPCIÓN ---
        if request.client_id not in self.suscripciones_cliente:
            with self.state_lock: # Proteger el diccionario de suscripciones
                if request.elecion_envio == True: # Cliente quiere DOS colas
                    # Elegir 2 colas únicas y guardarlas
                    colas_elegidas = random.sample([1, 2, 3], 2)
                    self.suscripciones_cliente[request.client_id] = colas_elegidas
                    # Imprimir el nuevo informe detallado
                    nombres = [self.nombres_colas[c] for c in colas_elegidas]
                    print(f"Nuevo cliente: {request.client_id} -> suscrito a dos colas ({' y '.join(nombres)})")
                
                else: # Cliente quiere UNA cola
                    # Elegir 1 cola y guardarla (en una lista)
                    colas_elegidas = [random.randint(1, 3)]
                    self.suscripciones_cliente[request.client_id] = colas_elegidas
                    # Imprimir el nuevo informe detallado
                    print(f"Nuevo cliente: {request.client_id} -> suscrito a una cola ({self.nombres_colas[colas_elegidas[0]]})")

        if self.stop_event.is_set():
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("El servidor está en proceso de apagado.")
            return publisher_pb2.envio_datos()
        
        try:
            # --- NUEVA LÓGICA DE GET ---
            # 1. Obtener las colas asignadas a este cliente
            colas_suscritas = self.suscripciones_cliente[request.client_id]
            
            # 2. Elegir una de esas colas al azar
            cola_a_usar = random.choice(colas_suscritas)
            
            # 3. Sacar de la cola seleccionada
            if(cola_a_usar == 1):
                task = self.cola_1.get(timeout=1.0)
            elif (cola_a_usar == 2):
                task = self.cola_2.get(timeout=1.0)
            else: # cola_a_usar == 3
                task = self.cola_3.get(timeout=1.0)
            
            return task
        
        except queue.Empty:
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(f"No hay tareas disponibles en la cola seleccionada, intente de nuevo.")
            return publisher_pb2.envio_datos()

    # --- RPC EnviarResultado (Sin cambios, ya estaba correcto) ---
    def EnviarResultado(self, request, context):
        """
        RPC 2: Un cliente envía el resultado de una tarea.
        """
        print(f"cliente {request.id_cliente} : {request.resultado}") # Opcional: muy verboso
        
        with self.state_lock:
            if self.stop_event.is_set():
                return publisher_pb2.resultados_server(fin_acciones=True, total_final=self.suma_total)

            # 1. Actualizar contadores
            self.operaciones_contador += 1
            self.suma_total += request.resultado
            self.contador_tareas_cliente[request.id_cliente] += 1
            
            should_stop = False

            # 2. Imprimir progreso
            if self.operaciones_contador % 10000 == 0:
                print(f"Progreso: {self.operaciones_contador:,} / {OPERACIONES_MAXIMAS:,} resultados. Suma total: {self.suma_total:,}")

            # 3. Comprobar la condición de parada (1 millón)
            if self.operaciones_contador >= OPERACIONES_MAXIMAS:
                if not self.stop_event.is_set():
                    print("\n--- ¡LÍMITE ALCANZADO! ---")
                    self.stop_event.set() 
                    should_stop = True
                    self._print_final_stats()
                    
                    threading.Thread(target=self._shutdown_server, daemon=True).start()

            # 4. Enviar respuesta al cliente
            return publisher_pb2.resultados_server(
                fin_acciones=should_stop,
                total_final=self.suma_total
            )

    # --- MODIFICADO: Informe Final ---
    def _print_final_stats(self):
        """Imprime el resumen final del trabajo."""
        print(f"Resultados totales procesados: {self.operaciones_contador:,}")
        print(f"Suma total agregada: {self.suma_total:,}")
        print(f"Vectores únicos generados: {len(self.vectores_unicos):,}")
        
        print("\n--- Clientes que trabajaron (Top 10) ---")
        for client_id, count in self.contador_tareas_cliente.most_common(10):
            print(f"  - {client_id}: {count:,} tareas resueltas")
            
        # --- NUEVO: Imprimir suscripciones (Lógica actualizada) ---
        print("\n--- Suscripciones de Clientes ---")
        for client_id, colas_lista in self.suscripciones_cliente.items():
             # Convertir la lista de números [1, 2] a strings ["principal", "secundaria"]
             nombres_colas = [self.nombres_colas[c] for c in colas_lista]
             
             if len(nombres_colas) > 1:
                 tipo_suscripcion = f"dos colas ({' y '.join(nombres_colas)})"
             else:
                 tipo_suscripcion = f"una cola ({nombres_colas[0]})"
                 
             print(f"  - {client_id}: suscrito a {tipo_suscripcion}")

    def _shutdown_server(self):
        """Detiene la instancia del servidor gRPC."""
        if self.server_instance:
            print("Iniciando apagado del servidor gRPC...")
            self.server_instance.stop(5)