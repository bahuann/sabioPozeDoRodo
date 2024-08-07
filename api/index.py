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
        CREATE TABLE IF NOT EXISTS companyTrips (
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
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS driver_advances (
            id SERIAL PRIMARY KEY,
            cpf_motorista VARCHAR(11) NOT NULL,
            id_ultima_corrida VARCHAR(50) NOT NULL,
            chave_pix_motorista VARCHAR(100) NOT NULL,
            valor_antecipacao FLOAT NOT NULL,
            request_time TIMESTAMP NOT NULL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders_history (
            id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            payment_method VARCHAR(50) NOT NULL,
            value FLOAT NOT NULL,
            order_time TIMESTAMP NOT NULL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS driver_rides (
            id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            driver_id VARCHAR(50) NOT NULL,
            order_amt FLOAT NOT NULL,
            order_fee_amt FLOAT NOT NULL,
            city_nm VARCHAR(50) NOT NULL,
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
    valid_api_keys = ["R//ZD'!95D&&4EG"]
    return api_key in valid_api_keys

@app.route('/companyTrips', methods=['GET'])
def get_companyTrips():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM companyTrips')
    trips = cursor.fetchall()
    cursor.close()
    conn.close()

    result = []
    for trip in trips:
        result.append({
            "id": trip[0],
            "serie": trip[1],
            "numero_do_conhecimento": trip[2],
            "data_de_emissao": trip[3].isoformat(),
            "cnpj_do_tomador": trip[4],
            "cliente": trip[5],
            "taxid_destinatario": trip[6],
            "razao_social_do_destinatario": trip[7],
            "ie_do_destinatario": trip[8],
            "identificador_do_cte": trip[9],
            "status_do_cte": trip[10],
            "valor_total_do_cte": trip[11],
            "cpf_motorista": trip[12]
        })
    return jsonify(result), 200

@app.route('/companyTrips', methods=['POST'])
def add_companyTrip():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        new_trip = request.json
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO companyTrips (serie, numero_do_conhecimento, data_de_emissao, cnpj_do_tomador, cliente, taxid_destinatario, razao_social_do_destinatario, ie_do_destinatario, identificador_do_cte, status_do_cte, valor_total_do_cte, cpf_motorista)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            new_trip['serie'],
            new_trip['numero_do_conhecimento'],
            datetime.fromisoformat(new_trip['data_de_emissao']),
            new_trip['cnpj_do_tomador'],
            new_trip['cliente'],
            new_trip.get('taxid_destinatario'),
            new_trip['razao_social_do_destinatario'],
            new_trip.get('ie_do_destinatario'),
            new_trip['identificador_do_cte'],
            new_trip['status_do_cte'],
            new_trip['valor_total_do_cte'],
            new_trip.get('cpf_motorista')
        ))
        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()

        new_trip['id'] = new_id
        return jsonify(new_trip), 201

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'message': 'Transaction received but there was an issue processing it.'}), 200

@app.route('/advance-request', methods=['POST'])
def request_advance():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        advance_request = request.json
        cpf_motorista = advance_request['cpf_motorista']
        id_ultima_corrida = advance_request['id_ultima_corrida']
        chave_pix_motorista = advance_request['chave_pix_motorista']
        valor_antecipacao = advance_request['valor_antecipacao']
        request_time = datetime.now()

        # Insert advance request data into the database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO driver_advances (cpf_motorista, id_ultima_corrida, chave_pix_motorista, valor_antecipacao, request_time)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        ''', (cpf_motorista, id_ultima_corrida, chave_pix_motorista, valor_antecipacao, request_time))
        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            "status": "Sucesso",
            "mensagem": "A antecipação foi aprovada e o valor será creditado na conta Pix do motorista em breve.",
            "id": new_id
        }), 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'error': 'An error occurred while processing the advance request'}), 400

@app.route('/orders-history', methods=['POST'])
def add_orders_history():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        order_data = request.json
        order_id = order_data['order_id']
        payment_method = order_data['payment_method']
        value = order_data['value']
        order_time = datetime.fromisoformat(order_data['order_time'])

        # Insert order data into the database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO orders_history (order_id, payment_method, value, order_time)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        ''', (order_id, payment_method, value, order_time))
        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            "status": "Sucesso",
            "mensagem": "Histórico do pedido foi registrado com sucesso.",
            "id": new_id
        }), 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'error': 'An error occurred while processing the order data'}), 400
    except ValueError as ve:
        print(f"ValueError: {ve}")
        return jsonify({'error': 'Invalid input data format'}), 400
    except psycopg2.DatabaseError as de:
        print(f"DatabaseError: {de}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return jsonify({'error': 'An unexpected error occurred'}), 400


@app.route('/driver-rides', methods=['POST'])
def add_driver_ride():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        ride_data = request.json
        print(f"Received ride data: {ride_data}")

        driver_id = ride_data['driver_id']
        order_id = ride_data['order_id']
        order_amt = float(ride_data['order_amt'])
        order_fee_amt = float(ride_data['order_fee_amt'])
        city_nm = ride_data['city_nm']
        order_start_dttm = datetime.fromisoformat(ride_data['order_start_dttm'])
        order_end_dttm = datetime.fromisoformat(ride_data['order_end_dttm'])
        order_dt = datetime.fromisoformat(ride_data['order_dt'])

        print(f"Processed data - driver_id: {driver_id}, order_id: {order_id}, order_amt: {order_amt}, order_fee_amt: {order_fee_amt}, city_nm: {city_nm}, order_start_dttm: {order_start_dttm}, order_end_dttm: {order_end_dttm}, order_dt: {order_dt}")

        # Insert driver ride data into the database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO driver_rides (driver_id, order_id, order_amt, order_fee_amt, city_nm, order_start_dttm, order_end_dttm, order_dt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (driver_id, order_id, order_amt, order_fee_amt, city_nm, order_start_dttm, order_end_dttm, order_dt))
        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            "status": "Success",
            "mensagem": "Driver trip recorded in database.",
            "id": new_id
        }), 200

    except ValueError as ve:
        print(f"ValueError: {ve}")
        return jsonify({'error': 'Invalid input data format'}), 400
    except psycopg2.DatabaseError as de:
        print(f"DatabaseError: {de}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return jsonify({'error': 'An unexpected error occurred'}), 400


if __name__ == '__main__':
    app.run(debug=True)
