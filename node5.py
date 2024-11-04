import socket
import os

NODE_NAME = 'node5'
NODE_PORT = 65436  # Porta do nó
NODE_ADDRESS = '0.0.0.0'  # Endereço do nó
SERVER_ADDRESS = '127.0.0.1'  # Endereço do servidor principal
SERVER_PORT = 12345  # Porta do servidor principal

def handle_node(client_socket):
    header_size = client_socket.recv(4).decode()
    while True:
        try:
            header = client_socket.recv(1024).decode()
            if not header:
                break
            
            if header.startswith('upload'):
                print(f"{header}")
                filename = header.split()[1]
                file_path = os.path.join(f"{NODE_NAME}_images", filename)

                client_socket.send("OK".encode())
                with open(file_path, 'wb') as file:
                    received_bytes = 0
                    while True:
                        bytes_read = client_socket.recv(8192)
                        if not bytes_read:
                            break
                        file.write(bytes_read)

                print(f"Imagem {filename} recebida e salva em {file_path}.")
                
            elif header.startswith('download'):
                filename = header.split()[1]
                file_path = os.path.join(f"{NODE_NAME}_images", filename)
                if os.path.exists(file_path):
                    client_socket.send(f"OK {os.path.getsize(file_path)}".encode())
                    with open(file_path, 'rb') as file:
                        while True:
                            bytes_read = file.read(8192)
                            if not bytes_read:
                                break
                            client_socket.send(bytes_read)
                else:
                    client_socket.send("Erro: Imagem não encontrada.".encode())
            
            elif header.startswith('delete'):
                filename = header.split()[1]
                file_path = os.path.join(f"{NODE_NAME}_images", filename)
                if os.path.exists(file_path):
                    os.remove(file_path)
                    client_socket.send(f"Imagem {filename} deletada com sucesso.".encode())
                else:
                    client_socket.send("Erro: Imagem não encontrada.".encode())

        except Exception as e:
            print(f"Erro: {e}")
            break
    client_socket.close()

def start_node():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        try:
            server_socket.connect((SERVER_ADDRESS, SERVER_PORT))
            active_message = f"Estou ativo como {NODE_NAME}, endereço {NODE_ADDRESS}, porta {NODE_PORT}."
            header = f"{len(active_message):04}".encode()
            server_socket.send(header + active_message.encode())
            print(f"{NODE_NAME} enviou a mensagem de ativação para o servidor principal.")
        except Exception as e:
            print(f"Erro ao conectar ao servidor: {e}")
            return

    print(f"Iniciando o nó {NODE_NAME}...")
    node_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_server.bind((NODE_ADDRESS, NODE_PORT))
    node_server.listen(5)
    print(f"Nó {NODE_NAME} escutando em {NODE_ADDRESS}:{NODE_PORT}")

    while True:
        client_socket, addr = node_server.accept()
        print(f"Conectado a {addr}")
        handle_node(client_socket)

if __name__ == "__main__":
    start_node()
