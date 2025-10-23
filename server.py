from concurrent import futures
import grpc
import sys
import os
# Añadir el directorio raíz del proyecto al path de Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from GRPC_Productor_Consumidor.proto import productor_pb2
from GRPC_Productor_Consumidor.proto import productor_pb2_grpc

# Importar la implementación del servicio (la nueva)
from servicios.servicios import WorkerCoordinatorServicer

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    
    # 1. Instanciar nuestro servicer
    servicer = WorkerCoordinatorServicer()
    
    # 2. Pasar la instancia del servidor al servicer (para el apagado)
    servicer.set_server_instance(server)
    
    # 3. Registrar el servicer en el servidor gRPC
    productor_pb2_grpc.add_WorkCoordinatorServicer_to_server(servicer, server)
    
    server.add_insecure_port('localhost:8000')
    server.start()
    print("✅ Servidor gRPC (WorkCoordinator) escuchando en el puerto 8000...")
    
    server.wait_for_termination()
    
    print("🛑 Servidor detenido. Adiós.")
    
if __name__ == '__main__':
    serve()