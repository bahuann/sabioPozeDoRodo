from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

# Configurações do banco de dados - substitua pelos seus valores
DB_HOST = "datasources.postgres.database.azure.com"
DB_NAME = "db_smart_capital_api_prd"
DB_USER = "a55dba"
DB_PASS = "Bzjlep2010#lima"

def get_db_connection():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    return conn

@app.route('/total_orders_by_status')
def total_orders_by_status():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT status, COUNT(*) AS number_of_orders FROM orders GROUP BY status;")
    results = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(results)

@app.route('/average_time_by_status')
def average_time_by_status():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT o.status, AVG(age(h.created_at, o.created_at)) AS average_duration FROM orders o INNER JOIN orders_histories h ON o.id = h.order_id GROUP BY o.status;")
    results = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
