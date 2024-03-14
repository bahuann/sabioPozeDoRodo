from flask import Flask, jsonify
import psycopg2
import psycopg2.extras

app = Flask(__name__)

# Database settings - replace with your values
DB_HOST = "datasources.postgres.database.azure.com"
DB_NAME = "db_smart_capital_api_prd"
DB_USER = "a55dba"
DB_PASS = "Bzjlep2010#lima"

@app.route('/')
def hello_world():
    return "Hello, World!"

@app.route('/test_db_connection')
def test_db_connection():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
        conn.close()
        return jsonify({"message": "Successfully connected to the database!"})
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500
        
def get_db_connection():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
        return conn
    except psycopg2.Error as e:
        return str(e)

@app.route('/total_orders_by_status')
def total_orders_by_status():
    conn = get_db_connection()
    if isinstance(conn, str):
        return jsonify({"error": conn}), 500

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cur.execute("SELECT status, COUNT(*) AS number_of_orders FROM orders GROUP BY status;")
        results = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify([dict(row) for row in results])
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500

@app.route('/average_time_by_status')
def average_time_by_status():
    conn = get_db_connection()
    if isinstance(conn, str):
        return jsonify({"error": conn}), 500

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cur.execute("SELECT o.status, AVG(age(h.created_at, o.created_at)) AS average_duration FROM orders o INNER JOIN orders_histories h ON o.id = h.order_id GROUP BY o.status;")
        results = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify([dict(row) for row in results])
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
