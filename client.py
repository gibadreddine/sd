import socket
import os

def list_client_images(directory):
    try:
        files = [f for f in os.listdir(directory) if f.endswith('.tif')]
        if not files:
            print("Nenhuma imagem encontrada no diretório do cliente.")
        else:
            print("Imagens no diretório do cliente:")
            for file in files:
                print(f"- {file}")
        return files
    except Exception as e:
        print(f"Erro ao listar arquivos no diretório do cliente: {e}")
        return []

def upload_image(client, filename):
    file_path = os.path.join('client_images', filename)

    if not os.path.exists(file_path):
        print("Imagem não encontrada. Tente novamente.")
        return

    header = f"upload {filename}"
    header_bytes = header.encode()
    count = 0
    try:
        client.send(f"{len(header_bytes):04}".encode())
        client.send(header_bytes)

        server_response = client.recv(1024).decode()
        if server_response.startswith("OK"):
            with open(file_path, 'rb') as f:
                while True:
                    count = count + 1
                    bytes_read = f.read(8192)
                    if not bytes_read:
                        break
                    client.send(bytes_read)
                    print(f"{count}")
            print(f"Imagem {filename} enviada com sucesso.")
        else:
            print(f"Erro: {server_response}")

    except Exception as e:
        print(f"Erro ao enviar a imagem: {e}")

def download_image(client_socket, filename):
    # Envia o pedido de download para o servidor
    download_request = f"download {filename}"
    client_socket.send(f"{len(download_request):04}".encode())
    client_socket.send(download_request.encode())

    # Espera a confirmação de que o servidor está pronto para enviar o arquivo
    response = client_socket.recv(1024).decode()
    if response.startswith("OK"):
        file_size = int(response.split()[1])  # Tamanho do arquivo fornecido pelo servidor
        file_path = os.path.join("client_images", filename)
        with open(file_path, 'wb') as file:
            total_received = 0
            while total_received < file_size:
                chunk = client_socket.recv(8192)
                if not chunk:
                    break
                file.write(chunk)
                total_received += len(chunk)
            print(f"Arquivo {filename} baixado com sucesso e salvo em {file_path}.")
    else:
        print(f"Erro ao baixar o arquivo: {response}")

def list_images(client):
    header = 'list'
    header_bytes = header.encode()

    try:
        client.send(f"{len(header_bytes):04}".encode())
        client.send(header_bytes)

        response = client.recv(4096).decode()
        print("Imagens disponíveis no servidor:")
        print(response)

    except Exception as e:
        print(f"Erro ao listar imagens: {e}")

def delete_image(client, filename):
    header = f"delete {filename}"
    header_bytes = header.encode()

    try:
        client.send(f"{len(header_bytes):04}".encode())
        client.send(header_bytes)

        response = client.recv(1024).decode()
        print(response)

    except Exception as e:
        print(f"Erro ao deletar imagem: {e}")


def start_client(host='127.0.0.1', port=12345):
    while True:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((host, port))

        try:
            print("Escolha uma opção:")
            print("1. Upload")
            print("2. Download")
            print("3. Listar imagens")
            print("4. Deletar imagem")
            print("5. Sair")
            option = input("Digite a opção desejada: ")

            if option == '5':
                print("Saindo...")
                break

            elif option == '1':  # Upload
                filename = input("Digite o nome da imagem (incluindo a extensão) para upload: ")
                upload_image(client, filename)
                

            elif option == '2':  # Download
                directory = 'client_images'  # Diretório do cliente
                list_client_images(directory)  # Exibe as imagens locais

                filename = input("Digite o nome da imagem que deseja baixar: ")
                
                # Verifica se a imagem já existe localmente antes de tentar baixar
                if filename in list_client_images(directory):
                    print(f"A imagem '{filename}' já existe no diretório do cliente.")
                else:
                    download_image(client, filename)


            elif option == '3':  # Listar Arquivos
                list_images(client)

            elif option == '4':  # Deletar arquivo
                filename = input("Digite o nome da imagem que deseja deletar: ")
                delete_image(client, filename)

        finally:
            client.close()

if __name__ == "__main__":
    start_client()

