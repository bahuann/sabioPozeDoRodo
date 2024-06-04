from flask import Flask, request, jsonify
import psycopg2
import os
from datetime import datetime

app = Flask(__name__)

# Configuration
DATABASE_URL = os.getenv('POSTGRES_URL')

def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    return conn

# Define a helper function to initialize the database
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            serie VARCHAR(50) NOT NULL,
            numero_do_conhecimento VARCHAR(50) NOT NULL,
            data_de_emissao TIMESTAMP NOT NULL,
            cnpj_do_tomador VARCHAR(20) NOT NULL,
            cliente VARCHAR(100) NOT NULL,
            taxid_destinatario VARCHAR(20),
            razao_social_do_destinatario VARCHAR(100) NOT NULL,
            ie_do_destinatario VARCHAR(20),
            identificador_do_cte VARCHAR(50) NOT NULL,
            status_do_cte VARCHAR(50) NOT NULL,
            valor_total_do_cte FLOAT NOT NULL,
            cpf_motorista VARCHAR(11)
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()

# Initialize the database
init_db()

def validate_api_key(api_key):
    # Implement your API key validation logic here
    valid_api_keys = ["R//ZD'!95D&&4EG"]
    return api_key in valid_api_keys

@app.route('/transactions', methods=['GET'])
def get_transactions():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM transactions')
    transactions = cursor.fetchall()
    cursor.close()
    conn.close()

    result = []
    for transaction in transactions:
        result.append({
            "id": transaction[0],
            "serie": transaction[1],
            "numero_do_conhecimento": transaction[2],
            "data_de_emissao": transaction[3].isoformat(),
            "cnpj_do_tomador": transaction[4],
            "cliente": transaction[5],
            "taxid_destinatario": transaction[6],
            "razao_social_do_destinatario": transaction[7],
            "ie_do_destinatario": transaction[8],
            "identificador_do_cte": transaction[9],
            "status_do_cte": transaction[10],
            "valor_total_do_cte": transaction[11],
            "cpf_motorista": transaction[12]
        })
    return jsonify(result), 200

@app.route('/transactions', methods=['POST'])
def add_transaction():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        new_transaction = request.json
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO transactions (serie, numero_do_conhecimento, data_de_emissao, cnpj_do_tomador, cliente, taxid_destinatario, razao_social_do_destinatario, ie_do_destinatario, identificador_do_cte, status_do_cte, valor_total_do_cte, cpf_motorista)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            new_transaction['serie'],
            new_transaction['numero_do_conhecimento'],
            datetime.fromisoformat(new_transaction['data_de_emissao']),
            new_transaction['cnpj_do_tomador'],
            new_transaction['cliente'],
            new_transaction.get('taxid_destinatario'),
            new_transaction['razao_social_do_destinatario'],
            new_transaction.get('ie_do_destinatario'),
            new_transaction['identificador_do_cte'],
            new_transaction['status_do_cte'],
            new_transaction['valor_total_do_cte'],
            new_transaction.get('cpf_motorista')
        ))
        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()

        new_transaction['id'] = new_id
        return jsonify(new_transaction), 201

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'message': 'Transaction received but there was an issue processing it.'}), 200

if __name__ == '__main__':
    app.run(debug=True)
