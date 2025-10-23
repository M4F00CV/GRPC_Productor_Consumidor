import grpc
import os
import sys
from concurrent import futures

# Añadir el directorio raíz del proyecto al path de Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from publisher_suscriptor.proto import publisher_pb2_grpc
from publisher_suscriptor.servicios.servicios import publicador

# --- NUEVA SECCIÓN DE VALIDACIÓN ---
def validar_argumento():
    """Valida el argumento de línea de comandos para el modo de selección."""
    modos_validos = ['aleatorio', 'ponderado', 'condicional']
    
    if len(sys.argv) < 2:
        print(f"Error: Debes especificar un modo de selección.")
        print(f"Uso: python {sys.argv[0]} <modo>")
        print(f"Modos válidos: {', '.join(modos_validos)}")
        sys.exit(1)
        
    modo = sys.argv[1].lower()
    
    if modo not in modos_validos:
        print(f"Error: Modo '{modo}' no es válido.")
        print(f"Modos válidos: {', '.join(modos_validos)}")
        sys.exit(1)
        
    return modo
# --- FIN DE LA NUEVA SECCIÓN ---

def serve(modo_seleccion: str):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    
    # 1. Instanciar nuestro servicer (MODIFICADO)
    # Pasa el modo de selección al constructor del servicer
    servicer = publicador(modo_seleccion=modo_seleccion) 
    
    # 2. Pasar la instancia del servidor al servicer (sin cambios)
    servicer.set_server_instance(server)
    
    # 3. Registrar el servicer en el servidor gRPC (sin cambios)
    publisher_pb2_grpc.add_publicadorServicer_to_server(servicer, server)
    
    server.add_insecure_port('localhost:8000')
    server.start()
    print(f"✅ Servidor gRPC (publicador) escuchando en el puerto 8000...")
    
    server.wait_for_termination()
    
    print("🛑 Servidor detenido. Adiós.")
    
if __name__ == '__main__':
    # Valida el argumento y luego inicia el servidor
    modo = validar_argumento()
    serve(modo_seleccion=modo)