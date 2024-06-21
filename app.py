from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2 import Error
import json
from decimal import Decimal

app = FastAPI()


def conectar_redshift():
    try:
        # Conecte-se ao Redshift
        connection = psycopg2.connect(
            user="",
            password="",
            host="",
            port="",
            database=""
        )
        return connection
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar ao Redshift: {e}")


def listar_dados_cashless_facts(connection, id_client, date_order_inicial, date_order_final):
    try:
        cursor = connection.cursor()
        
      

        query = "SELECT ...;"
        cursor.execute(query, (id_client, date_order_inicial, date_order_final))

        records = cursor.fetchall()

        produtos = []

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

        return produtos
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar dados da tabela: {e}")

@app.get("/...")
def listar_dados(id_client: str, date_order_inicial: str, date_order_final: str):

    connection = conectar_redshift()
    if connection:
        try:

            dados = listar_dados_cashless_facts(connection, id_client, date_order_inicial, date_order_final)
            print(dados)
            return dados
        finally:

            connection.close()
    else:
        raise HTTPException(status_code=500, detail="Falha ao conectar ao banco de dados")
    

@app.get("/p...........")
def listar_dados():

    connection = conectar_redshift()
    if connection:
        try:

            dados = listar_produtos(connection)
            return dados
        finally:

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
        query = "SELECT ......."
        cursor.execute(query)

        records = cursor.fetchall()
        

        produtos = []
        for record in records:
            produto = {
                'codigoProduto': record[0],
                'nome': record[1],
                'valor': float(record[2]), 
            }
            produtos.append(produto)
        
        return produtos
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar dados da tabela: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=777777777)

