from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2 import Error
import json
from decimal import Decimal

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}


def conectar_redshift():
    try:
        # Conecte-se ao Redshift
        connection = psycopg2.connect(
            user="eduardoabreu",
            password="5Su8ljjp9pQH",
            host="smartbreak-wg.770131890067.us-east-2.redshift-serverless.amazonaws.com",
            port="5439",
            database="smartbreak-rs-db"
        )
        return connection
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar ao Redshift: {e}")


def listar_dados_cashless_facts(connection, id_client, date_order_inicial, date_order_final):
    try:
        cursor = connection.cursor()
        
        # Execute uma consulta SQL para selecionar todos os dados da tabela        
        # query = "SELECT id_client,des_good_name, qtd_items, vlr_price, des_status FROM trusted_vmpay.tb_cashless_facts where id_client = %s and dat_order BETWEEN %s AND %s;"

        query = "SELECT id_client,des_good_name, qtd_items, vlr_price, des_status, cod_machine_asset_number FROM trusted_vmpay.tb_cashless_facts where id_client = %s and dat_order BETWEEN %s AND %s;"
        cursor.execute(query, (id_client, date_order_inicial, date_order_final))
        # Obtenha todos os resultados
        records = cursor.fetchall()
        # Converta os resultados em uma lista de dicionários
        produtos = []
        # print(records)  
        for record in records:
            produto = {
                'id': record[0],
                'nome': record[1],
                'quantidade': record[2],
                'valor': float(record[3]), 
                'status': record[4],
                'maquina': record[5],
            }
            produtos.append(produto)
        # print(produtos)
        return produtos
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar dados da tabela: {e}")

@app.get("/cashlessfacts/loja/{id_client}/data-inicial/{date_order_inicial}/data-final/{date_order_final}")
def listar_dados(id_client: str, date_order_inicial: str, date_order_final: str):
    # Conecte-se ao Redshift
    connection = conectar_redshift()
    if connection:
        try:
            # Liste os dados da tabela
            dados = listar_dados_cashless_facts(connection, id_client, date_order_inicial, date_order_final)
            print(dados)
            return dados
        finally:
            # Feche a conexão após obter os dados
            connection.close()
    else:
        raise HTTPException(status_code=500, detail="Falha ao conectar ao banco de dados")
    

@app.get("/produtos/")
def listar_dados():
    # Conecte-se ao Redshift
    connection = conectar_redshift()
    if connection:
        try:
            # Liste os dados da tabela
            dados = listar_produtos(connection)
            return dados
        finally:
            # Feche a conexão após obter os dados
            connection.close()
    else:
        raise HTTPException(status_code=500, detail="Falha ao conectar ao banco de dados")
    
def serialize_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

import json
from decimal import Decimal

def listar_produtos(connection):
    try:
        cursor = connection.cursor()
        # Execute uma consulta SQL para selecionar todos os dados da tabela
        query = "SELECT id_good_item as id, des_item_good_name as nome, vlr_item_desired_price as valor FROM trusted_vmpay.tb_planograms where id_planogram = 525437"
        cursor.execute(query)
        # Obtenha todos os resultados
        records = cursor.fetchall()
        
        # Converta os resultados em uma lista de dicionários
        produtos = []
        for record in records:
            produto = {
                'codigoProduto': record[0],
                'nome': record[1],
                'valor': float(record[2]), # Convertendo para float
            }
            produtos.append(produto)
        
        return produtos
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar dados da tabela: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

