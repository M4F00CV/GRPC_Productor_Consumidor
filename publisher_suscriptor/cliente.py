import grpc
import uuid
import time
import random 
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from publisher_suscriptor.proto import publisher_pb2
from publisher_suscriptor.proto import publisher_pb2_grpc

def eleccion_colas():
    x=random.randint(1,100)
    if(x<=50): return True #dos colas
    else: return False #una cola

def run_worker():
    ELECCOIN_COLAS = eleccion_colas()
    client_id = f"worker_client_{str(uuid.uuid4())[:8]}"
    print(f"🚀 Iniciando cliente: {client_id}")

    with grpc.insecure_channel('localhost:8000') as channel:
        stub = publisher_pb2_grpc.publicadorStub(channel)
        
        # CREAR EL MENSAJE DE PETICIÓN UNA SOLA VEZ
        # (ya que el client_id no cambia)
        task_request = publisher_pb2.reconocer_cliente(client_id=client_id,elecion_envio=ELECCOIN_COLAS)
        
        tasks_solved = 0
        
        while True:
            try:
                # 1. Pedir una tarea al servidor
                print(f"[{client_id}] Pidiendo tarea...")
                
                # MODIFICADO: Pasa el objeto 'task_request'
                task = stub.PedirDatos(task_request)

                if not task.rastreador:
                    print(f"[{client_id}] No se recibió tarea, esperando...")
                    time.sleep(1)
                    continue

                print(f"[{client_id}] Tarea {task.rastreador} recibida: {task.num1}, {task.num2}, {task.num3}")

                # 2. "Resolver" la tarea (la función matemática: suma)
                time.sleep(random.uniform(0.01, 0.1)) 
                result = task.num1 + task.num2 + task.num3

                # 3. Crear el mensaje de resultado (esto no cambia)
                result_msg = publisher_pb2.resultado_cliente(
                    rastreador=task.rastreador,
                    id_cliente=client_id,
                    resultado=result  # <-- 'result' (tu variable) se asigna a 'num1' (campo del proto)
                )

                # 4. Enviar el resultado de vuelta (esto no cambia)
                response = stub.EnviarResultado(result_msg)
                tasks_solved += 1
                print(f"[{client_id}] Resultado enviado. Tareas resueltas: {tasks_solved}. Suma total del server: {response.total_final:,}")

                # 5. Comprobar si el servidor nos dijo que paráramos (esto no cambia)
                if response.fin_acciones:
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