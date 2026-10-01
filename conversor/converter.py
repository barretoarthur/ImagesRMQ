import pika
import io
import time
from PIL import Image

RABBITMQ_HOST = 'rabbitmq'
FILA_ENTRADA = 'fila_nao_processadas'
EXCHANGE_SAIDA = 'exchange_processadas'

def conectar_rabbitmq():
    # Aguarda o RabbitMQ estar pronto 
    time.sleep(10)
    conexao = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    canal = conexao.channel()
    return conexao, canal

def processar_imagem(ch, method, properties, body):
    nome_original = properties.headers.get('nome_original')
    print(f"[Conversor] Processando a imagem: {nome_original}")

    imagem_original = Image.open(io.BytesIO(body))
    imagem_cinza = imagem_original.convert('L') # L é o modo para escala de cinza no Pillow

    buffer = io.BytesIO()
    imagem_cinza.save(buffer, format=imagem_original.format or 'JPEG')
    bytes_imagem_cinza = buffer.getvalue()

    ch.basic_publish(
        exchange=EXCHANGE_SAIDA,
        routing_key='', 
        body=bytes_imagem_cinza,
        properties=pika.BasicProperties(
            headers={'nome_original': nome_original} 
        )
    )
    

    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"[Conversor] Imagem {nome_original} convertida e enviada para o Exchange.")

def iniciar_conversor():
    conexao, canal = conectar_rabbitmq()

    # Garante que a fila de entrada existe (caso o conversor inicie antes do cliente)
    canal.queue_declare(queue=FILA_ENTRADA)
    
    # Cria o Exchange 'X' do tipo 'fanout'. 
    canal.exchange_declare(exchange=EXCHANGE_SAIDA, exchange_type='fanout')

    canal.basic_qos(prefetch_count=1)
    canal.basic_consume(queue=FILA_ENTRADA, on_message_callback=processar_imagem)

    print("[Conversor] Aguardando imagens. Para sair, pressione CTRL+C")
    canal.start_consuming()

if __name__ == '__main__':
    iniciar_conversor()