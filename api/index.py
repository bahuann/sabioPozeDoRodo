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
            driver_id VARCHAR(50) NOT NULL,
            order_id VARCHAR(50) NOT NULL,
            order_amt FLOAT NOT NULL,
            order_fee_amt FLOAT NOT NULL,
            city_nm VARCHAR(100) NOT NULL,
            order_start_dttm TIMESTAMP NOT NULL,
            order_end_dttm TIMESTAMP NOT NULL,
            order_dt DATE NOT NULL
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()

# Initialize the database
init_db()

def validate_api_key(api_key):
    # Implement your API key validation logic here
    valid_api_keys = ["your-secure-api-key"]
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
            "driver_id": transaction[1],
            "order_id": transaction[2],
            "order_amt": transaction[3],
            "order_fee_amt": transaction[4],
            "city_nm": transaction[5],
            "order_start_dttm": transaction[6].isoformat(),
            "order_end_dttm": transaction[7].isoformat(),
            "order_dt": transaction[8].isoformat()
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
            INSERT INTO transactions (driver_id, order_id, order_amt, order_fee_amt, city_nm, order_start_dttm, order_end_dttm, order_dt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            new_transaction['driver_id'],
            new_transaction['order_id'],
            new_transaction['order_amt'],
            new_transaction['order_fee_amt'],
            new_transaction['city_nm'],
            datetime.fromisoformat(new_transaction['order_start_dttm']),
            datetime.fromisoformat(new_transaction['order_end_dttm']),
            datetime.fromisoformat(new_transaction['order_dt']).date()
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
