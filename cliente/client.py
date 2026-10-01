import pika
import os
import time

RABBITMQ_HOST = 'rabbitmq'
NOME_FILA = 'fila_nao_processadas' 
PASTA_IMAGENS = os.getenv('NOME_PASTA_CLIENTE', 'pasta_cliente1')

def conectar_rabbitmq():
    time.sleep(10) 

    conexao = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    canal = conexao.channel()
    # Declara a fila onde as imagens originais serão colocadas
    canal.queue_declare(queue=NOME_FILA)
    return conexao, canal

def enviar_imagens():
    conexao, canal = conectar_rabbitmq()
    
    # Percorre todos os ficheiros dentro da pasta do cliente
    for nome_ficheiro in os.listdir(PASTA_IMAGENS):
        caminho_completo = os.path.join(PASTA_IMAGENS, nome_ficheiro)
        
        # Verifica se é um ficheiro válido
        if os.path.isfile(caminho_completo):
            with open(caminho_completo, 'rb') as f:
                bytes_imagem = f.read()
                
            # Publica a mensagem na fila, enviando o nome do ficheiro nos headers
            canal.basic_publish(
                exchange='',
                routing_key=NOME_FILA,
                body=bytes_imagem,
                properties=pika.BasicProperties(
                    headers={'nome_original': nome_ficheiro}
                )
            )
            print(f"[Cliente] Imagem {nome_ficheiro} enviada para a fila.")
            
    conexao.close()

if __name__ == '__main__':
    enviar_imagens()