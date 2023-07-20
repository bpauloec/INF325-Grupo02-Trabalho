from cassandra.cluster import Cluster
import uuid
from datetime import datetime, timedelta
import random

# Endereços dos nós do Cassandra (substitua pelos endereços reais do seu cluster)
enderecos_cassandra = ['127.0.0.1']

# Porta padrão do Cassandra
porta_cassandra = 9042

# Chaves do keyspace e nome da tabela (substitua pelos valores reais)
keyspace = 'reclamacoes'
nome_tabela = 'complaint'

# Função para estabelecer a conexão com o cluster do Cassandra
def conectar_cassandra():
    try:
        cluster = Cluster(enderecos_cassandra, port=porta_cassandra)
        session = cluster.connect(keyspace=keyspace)
        return cluster, session
    except Exception as e:
        print(f"Erro ao conectar ao Cassandra: {e}")
        return None, None

# Função para executar uma query no Cassandra
def executar_query(session, query):
    try:
        session.execute(query)
    except Exception as e:
        print(f"Erro ao executar a query: {e}")

# Função para gerar um UUID aleatório
def gerar_uuid():
    return uuid.uuid4()

# Função para gerar uma data e hora aleatória nos últimos 365 dias
def gerar_data_hora():
    agora = datetime.now()
    delta = timedelta(days=random.randint(0, 365))
    return agora - delta

# Função para gerar uma descrição aleatória
def gerar_descricao():
    return f"Reclamação sobre {random.choice(['produto', 'atendimento', 'entrega'])}"

# Função para gerar um status aleatório
def gerar_status():
    return random.choice(['Aberto', 'Em andamento', 'Fechado'])

# Função para gerar um título aleatório
def gerar_titulo():
    return f"Reclamação {random.randint(1000, 9999)}"

# Função para gerar um tipo aleatório
def gerar_tipo():
    return random.choice(['Reclamação', 'Sugestão', 'Elogio', 'Dúvida'])

# Função para gerar um reply aleatório (timestamp e texto)
def gerar_reply():
    reply_date = gerar_data_hora()
    content = gerar_descricao()
    return (reply_date, content)

# Função para inserir 300 registros aleatórios na tabela
def inserir_dados_aleatorios(session, quantidade):
    for _ in range(quantidade):
        ticket_id = gerar_uuid()
        creation_date = gerar_data_hora()
        description = gerar_descricao()
        replies = {gerar_data_hora(): gerar_descricao() for _ in range(random.randint(1, 5))}
        status = gerar_status()
        title = gerar_titulo()
        type_ = gerar_tipo()
        user_id = gerar_uuid()

        query = f"INSERT INTO {nome_tabela} (ticket_id, creation_date, description, replies, status, title, type, user_id) " \
                f"VALUES ({ticket_id}, '{creation_date}', '{description}', {replies}, '{status}', '{title}', '{type_}', {user_id});"

        executar_query(session, query)

# Exemplo de uso
if __name__ == "__main__":
    # Estabelecer a conexão
    cluster, session = conectar_cassandra()

    if session:
        # Defina a quantidade de dados aleatórios que você deseja gerar e inserir
        quantidade_dados_aleatorios = 300

        # Inserir dados aleatórios na tabela
        inserir_dados_aleatorios(session, quantidade_dados_aleatorios)

        # Fechar a conexão
        cluster.shutdown()