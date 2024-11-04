import threading
import socket
import os
from collections import defaultdict

HOST = '127.0.0.1'  # Endereço do servidor principal
SERVER_PORT = 12345  # Porta do servidor principal

# Mapeamento de nós
node_mapping = defaultdict(list)
node_info = {}

# Taxa de replicação
replication_factor = 2  

current_node_index = 0
lock = threading.Lock()

def handle_client(client_socket):
    global current_node_index

    try:
        while True:
            header_size = client_socket.recv(4).decode()
            if not header_size:
                break

            header_size = int(header_size)
            header = client_socket.recv(header_size).decode()

            if header.startswith("Estou ativo como"):
                # Header quando são nós
                node_data = header.split(", ")
                node_name = node_data[0].split()[-1]
                node_address = node_data[1].split()[-1]
                node_port = node_data[2].split()[-1]

                if node_name not in node_info:
                    node_info[node_name] = (node_address, int(node_port))
                    print(f"Nó {node_name} adicionado: {node_address}:{node_port}")
                else:
                    print(f"Nó {node_name} já existe, não foi adicionado.")
                
                return

            elif header.lower().startswith('upload'):
                filename = header.split()[1]
                
                with lock:
                    if len(node_info) < replication_factor:
                        client_socket.send("Erro: Não há nós suficientes para replicação.".encode())
                        continue
                    
                    node_names = list(node_info.keys())
                    target_nodes = [node_names[(current_node_index + i) % len(node_info)] for i in range(replication_factor)]
                    current_node_index = (current_node_index + replication_factor) % len(node_info)

                client_socket.send("OK".encode())  # Confirmação para o cliente começar a enviar o arquivo

                node_sockets = []
                try:
                    # Abre uma conexão socket para cada nó de destino
                    for target_node_name in target_nodes:
                        node_ip, node_port = node_info[target_node_name]
                        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        node_socket.connect((node_ip, node_port))
                        node_header = f"upload {filename}"
                        node_header_bytes = node_header.encode()
                        node_socket.send(f"{len(node_header_bytes):04}".encode())
                        node_socket.send(node_header_bytes)

                        server_response = node_socket.recv(1024).decode()
                        if server_response.startswith("OK"):
                            node_sockets.append((node_socket, target_node_name))
                        else:
                            print(f"Erro no nó {target_node_name}: {server_response}")
                            node_socket.close()

                    if len(node_sockets) < replication_factor:
                        client_socket.send("Erro: Não foi possível conectar a todos os nós de destino.".encode())
                        continue

                    # Recebe o arquivo do cliente e retransmite para todos os nós de destino
                    while True:
                        chunk = client_socket.recv(8192)  # Recebe o arquivo em blocos
                        if not chunk:
                            break
                        for node_socket, node_name in node_sockets:
                            node_socket.send(chunk)  # Envia o bloco para cada nó

                    # Após o envio, registra os nós onde a imagem foi armazenada
                    for _, node_name in node_sockets:
                        node_mapping[filename].append(node_name)
                        print(f"Imagem {filename} enviada para {node_name} com sucesso.")

                    client_socket.send(f"Imagem {filename} enviada para os nós com sucesso.".encode())

                except Exception as e:
                    client_socket.send(f"Erro durante o upload: {e}".encode())
                    upload_success = False
                finally:
                    # Fecha todos os sockets abertos para os nós
                    for node_socket, _ in node_sockets:
                        node_socket.close()


            elif header.lower().startswith('download'):
                filename = header.split()[1]

                # Verifica se o arquivo está mapeado nos nós
                if filename not in node_mapping or not node_mapping[filename]:
                    client_socket.send("Erro: Imagem não encontrada.".encode())
                    continue

                # Obtém todos os nós que possuem a imagem
                target_nodes = node_mapping[filename]
                download_success = False

                for target_node in target_nodes:
                    node_ip, node_port = node_info[target_node]
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
                            node_socket.connect((node_ip, int(node_port)))
                            node_header = f"download {filename}"
                            node_header_bytes = node_header.encode()
                            node_socket.send(f"{len(node_header_bytes):04}".encode())
                            node_socket.send(node_header_bytes)

                            response = node_socket.recv(1024).decode()
                            if response.startswith("OK"):
                                file_size = int(response.split()[1])  # Tamanho do arquivo
                                client_socket.send(f"OK {file_size}".encode())  # Confirmação para o cliente

                                # Enviar os dados do arquivo para o cliente
                                while True:
                                    chunk = node_socket.recv(8192)
                                    if not chunk:
                                        break
                                    client_socket.send(chunk)
                                download_success = True
                                break  # Sai do loop se o download foi bem-sucedido
                            else:
                                print(f"Nó {target_node} retornou erro: {response}")

                    except Exception as e:
                        print(f"Erro ao conectar com nó {target_node}: {e}")
                
                if not download_success:
                    client_socket.send("Erro: Todos os nós disponíveis estão indisponíveis.".encode())

            elif header.startswith('delete'):
                filename = header.split()[1]

                if filename not in node_mapping or not node_mapping[filename]:
                    client_socket.send("Erro: Imagem não encontrada.".encode())
                    continue

                # Obtem todos os nós que possuem a imagem
                target_nodes = node_mapping[filename]
                deletion_success = True

                for target_node in target_nodes:
                    node_ip, node_port = node_info[target_node]
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
                            node_socket.connect((node_ip, int(node_port)))
                            node_header = f"delete {filename}"
                            node_header_bytes = node_header.encode()
                            node_socket.send(f"{len(node_header_bytes):04}".encode())
                            node_socket.send(node_header_bytes)

                            response = node_socket.recv(1024).decode()
                            print(f"Resposta do nó {target_node}: {response}")
                    except Exception as e:
                        print(f"Erro ao conectar com nó {target_node}: {e}")
                        deletion_success = False

                if deletion_success:
                    del node_mapping[filename]
                    client_socket.send(f"Imagem {filename} deletada com sucesso de todos os nós.".encode())
                else:
                    client_socket.send(f"Falha ao deletar a imagem {filename} em um ou mais nós.".encode())

            elif header.lower() == 'list':
                if not node_mapping:
                    client_socket.send("Nenhuma imagem encontrada.".encode())
                else:
                    unique_images = list(node_mapping.keys())
                    response = '\n'.join(unique_images)
                    client_socket.send(response.encode())

    except Exception as e:
        print(f"Erro no manuseio do cliente: {e}")
    finally:
        client_socket.close()

def start_server(host='0.0.0.0', port=SERVER_PORT):
    print("Iniciando o servidor...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    print(f"Servidor escutando em {host}:{port}")

    while True:
        client_socket, addr = server.accept()
        print(f"Conectado a {addr}")
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()

if __name__ == "__main__":
    start_server()

