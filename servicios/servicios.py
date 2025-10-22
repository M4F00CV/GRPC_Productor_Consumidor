import grpc
import random
import uuid
import threading
import queue
from collections import Counter
import time

import sys
import os
# Añadir el directorio raíz del proyecto al path de Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from GRPC_Productor_Consumidor.proto import productor_pb2
from GRPC_Productor_Consumidor.proto import productor_pb2_grpc


# Límite de resultados para detener el sistema
STOP_AFTER_N_RESULTS = 100

class WorkerCoordinatorServicer(productor_pb2_grpc.WorkCoordinatorServicer):
    """
    Implementación del servicio WorkCoordinator.
    Esta clase es "stateful", maneja el estado de la aplicación.
    """

    def __init__(self):
        # --- Estado Compartido ---
        self.task_queue = queue.Queue(maxsize=10000) # Cola de tareas pendientes
        self.generated_tasks_set = set() # Set para garantizar vectores únicos
        self.total_results = 0             # Contador de resultados recibidos
        self.total_sum = 0                 # Suma total de todos los resultados
        self.client_leaderboard = Counter()# Contador de tareas por client_id
        
        # --- Herramientas de Concurrencia ---
        self.state_lock = threading.Lock()   # Lock para proteger el estado compartido
        self.stop_event = threading.Event()  # Evento para detener todos los hilos
        self.server_instance = None          # Referencia al servidor gRPC para detenerlo
        
        # --- Hilo Generador de Tareas ---
        # Iniciamos un hilo en segundo plano que genera tareas
        self.generator_thread = threading.Thread(target=self._task_generator_loop, daemon=True)
        self.generator_thread.start()
        print("✅ Hilo generador de tareas iniciado.")

    def set_server_instance(self, server):
        """Permite al script principal pasar una referencia del servidor."""
        self.server_instance = server

    def _task_generator_loop(self):
        """
        Bucle ejecutado por el hilo generador.
        Genera tareas únicas y las añade a la cola.
        """
        while not self.stop_event.is_set():
            try:
                # 1. Generar 3 números
                n1 = random.randint(1, 1000)
                n2 = random.randint(1, 1000)
                n3 = random.randint(1, 1000)
                
                # 2. Crear un "vector" único (ordenándolos)
                task_tuple = tuple(sorted((n1, n2, n3)))

                # 3. Verificar unicidad
                if task_tuple not in self.generated_tasks_set:
                    self.generated_tasks_set.add(task_tuple)
                    
                    # 4. Crear el mensaje de la tarea
                    task_id = str(uuid.uuid4())
                    task_msg = productor_pb2.Task(
                        task_id=task_id,
                        num1=task_tuple[0],
                        num2=task_tuple[1],
                        num3=task_tuple[2]
                    )
                    
                    # 5. Añadir a la cola (con timeout para no bloquear)
                    self.task_queue.put(task_msg, timeout=0.5)
                    #print("Añadido : ",task_tuple)
                
            except queue.Full:
                # Si la cola está llena, los clientes no están consumiendo
                # lo suficientemente rápido. Esperamos.
                time.sleep(0.1)
            except Exception:
                # Ignorar otros errores (como el timeout de put) y continuar
                pass
        
        print("🛑 Hilo generador de tareas detenido.")

    # --- Implementación de los RPCs ---

    def GetTask(self, request, context):
        """
        RPC 1: Un cliente pide una tarea.
        El servidor saca una tarea de la cola y se la da.
        El 'request' ahora contiene el client_id.
        """ 
        #print(f"Petición de tarea recibida de: {request.client_id}")

        if self.stop_event.is_set():
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("El servidor está en proceso de apagado.")
            return productor_pb2.Task()
        try:
            task = self.task_queue.get(timeout=1.0)
            return task
        except queue.Empty:
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details("No hay tareas disponibles, intente de nuevo.")
            return productor_pb2.Task()

    def SubmitResult(self, request, context):
        """
        RPC 2: Un cliente envía el resultado de una tarea.
        El servidor actualiza sus contadores y el leaderboard.
        """
        print(f"cliente {request.client_id} : {request.result}")
        
        # Usamos un 'lock' para asegurar que el estado se actualice
        # de forma atómica (uno a la vez), previniendo "race conditions".
        with self.state_lock:
            # Si el servidor ya se detuvo, rechazar nuevos resultados
            if self.stop_event.is_set():
                return productor_pb2.SubmitResponse(stop_signal=True, total_server_sum=self.total_sum)

            # 1. Actualizar contadores
            self.total_results += 1
            self.total_sum += request.result
            self.client_leaderboard[request.client_id] += 1
            
            # Variable local para saber si debemos detenernos
            should_stop = False

            # 2. Imprimir progreso (cada 10,000 resultados)
            if self.total_results % 10000 == 0:
                print(f"Progreso: {self.total_results:,} / {STOP_AFTER_N_RESULTS:,} resultados. Suma total: {self.total_sum:,}")

            # 3. Comprobar la condición de parada
            if self.total_results >= STOP_AFTER_N_RESULTS:
                if not self.stop_event.is_set():
                    print("\n--- ¡LÍMITE ALCANZADO! ---")
                    self.stop_event.set() # Señal para detener todo
                    should_stop = True
                    self._print_final_stats()
                    
                    # Iniciar el apagado del servidor en un hilo separado
                    # para no bloquear esta respuesta
                    threading.Thread(target=self._shutdown_server, daemon=True).start()

            # 4. Enviar respuesta al cliente
            return productor_pb2.SubmitResponse(
                stop_signal=should_stop,
                total_server_sum=self.total_sum
            )

    # --- Métodos de Ayuda ---

    def _print_final_stats(self):
        """Imprime el resumen final del trabajo."""
        print(f"Resultados totales procesados: {self.total_results:,}")
        print(f"Suma total agregada: {self.total_sum:,}")
        print(f"Vectores únicos generados: {len(self.generated_tasks_set):,}")
        print("\n--- Leaderboard de Clientes (Top 10) ---")
        
        # Ordenar el Counter por el valor (conteo) de forma descendente
        for client_id, count in self.client_leaderboard.most_common(10):
            print(f"  - {client_id}: {count:,} tareas resueltas")

    def _shutdown_server(self):
        """Detiene la instancia del servidor gRPC."""
        if self.server_instance:
            print("Iniciando apagado del servidor gRPC...")
            # Damos 5 segundos de gracia para que terminen las peticiones
            self.server_instance.stop(5)