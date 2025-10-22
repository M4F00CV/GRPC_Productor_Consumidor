import grpc
import uuid
import time
import random 
import sys
import os
# Añadir el directorio raíz del proyecto al path de Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from GRPC_Productor_Consumidor.proto import productor_pb2
from GRPC_Productor_Consumidor.proto import productor_pb2_grpc


def run_worker():
    client_id = f"worker_client_{str(uuid.uuid4())[:8]}"
    print(f"🚀 Iniciando cliente: {client_id}")

    with grpc.insecure_channel('localhost:8000') as channel:
        stub = productor_pb2_grpc.WorkCoordinatorStub(channel)
        
        # CREAR EL MENSAJE DE PETICIÓN UNA SOLA VEZ
        # (ya que el client_id no cambia)
        task_request = productor_pb2.TaskRequest(client_id=client_id)
        
        tasks_solved = 0
        
        while True:
            try:
                # 1. Pedir una tarea al servidor
                print(f"[{client_id}] Pidiendo tarea...")
                
                # MODIFICADO: Pasa el objeto 'task_request'
                task = stub.GetTask(task_request)

                if not task.task_id:
                    print(f"[{client_id}] No se recibió tarea, esperando...")
                    time.sleep(1)
                    continue

                print(f"[{client_id}] Tarea {task.task_id} recibida: {task.num1}, {task.num2}, {task.num3}")

                # 2. "Resolver" la tarea (la función matemática: suma)
                time.sleep(random.uniform(0.01, 0.1)) 
                result = task.num1 + task.num2 + task.num3

                # 3. Crear el mensaje de resultado (esto no cambia)
                result_msg = productor_pb2.TaskResult(
                    task_id=task.task_id,
                    client_id=client_id,
                    result=result
                )

                # 4. Enviar el resultado de vuelta (esto no cambia)
                response = stub.SubmitResult(result_msg)
                tasks_solved += 1
                print(f"[{client_id}] Resultado enviado. Tareas resueltas: {tasks_solved}. Suma total del server: {response.total_server_sum:,}")

                # 5. Comprobar si el servidor nos dijo que paráramos (esto no cambia)
                if response.stop_signal:
                    print(f"[{client_id}] 🛑 Señal de STOP recibida del servidor. Terminando.")
                    break
            
            # ... (el manejo de errores no cambia) ...
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                    print(f"[{client_id}] No hay tareas disponibles. Reintentando en 2s...")
                    time.sleep(2)
                elif e.code() == grpc.StatusCode.UNAVAILABLE:
                    print(f"[{client_id}] 🛑 El servidor no está disponible. Terminando.")
                    break
                else:
                    print(f"[{client_id}] ❌ Error gRPC inesperado: {e.details()}")
                    break
            except Exception as e:
                print(f"[{client_id}] ❌ Error inesperado: {e}")
                break
                
        print(f"Cliente {client_id} terminado. Tareas totales resueltas: {tasks_solved}")

if __name__ == '__main__':
    run_worker()