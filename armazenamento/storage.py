import pika
import os
import time

RABBITMQ_HOST = 'rabbitmq'
EXCHANGE_SAIDA = 'exchange_processadas' # O mesmo Exchange 'X' usado no conversor

PASTA_ARMAZENAMENTO = os.getenv('NOME_PASTA_ARMAZENAMENTO', 'pasta_padrao')
NOME_FILA = f'fila_{PASTA_ARMAZENAMENTO}'

def salvar_imagem(ch, method, properties, body):
    nome_original = properties.headers.get('nome_original')
    if not nome_original:
        nome_original = f"imagem_sem_nome_{method.delivery_tag}.jpg"

    print(f"[Armazenador] Recebida a imagem processada: {nome_original}")

    if not os.path.exists(PASTA_ARMAZENAMENTO):
        os.makedirs(PASTA_ARMAZENAMENTO)

    caminho_completo = os.path.join(PASTA_ARMAZENAMENTO, nome_original)

    #  salva os bytes da imagem (já em tons de cinza) no disco
    with open(caminho_completo, 'wb') as f:
        f.write(body)

    print(f"[Armazenador] Imagem salva com sucesso em: {caminho_completo}")

def iniciar_armazenador():
    time.sleep(10)
    conexao = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    canal = conexao.channel()


    canal.exchange_declare(exchange=EXCHANGE_SAIDA, exchange_type='fanout')
    # fila nomeada pra esse server de armazemanento
    # Fila nomeada garante que mensagens não sejam perdidas caso o armazenador reinicie
    canal.queue_declare(queue=NOME_FILA)
    canal.queue_bind(exchange=EXCHANGE_SAIDA, queue=NOME_FILA)

    print(f"[Armazenador] Aguardando imagens na fila '{NOME_FILA}', salvando em '{PASTA_ARMAZENAMENTO}'...")
    
    # auto_ack=True pode ser usado aqui se não houver lógica complexa de falha após o recebimento
    canal.basic_consume(queue=NOME_FILA, on_message_callback=salvar_imagem, auto_ack=True)
    canal.start_consuming()

if __name__ == '__main__':
    iniciar_armazenador()