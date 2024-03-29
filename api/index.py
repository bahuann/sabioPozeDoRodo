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
    cur = conn.cursor()
    cur.execute("SELECT status, COUNT(*) AS number_of_orders FROM smart_capital_customer.orders GROUP BY status;")
    results = cur.fetchall()
    cur.close()
    conn.close()
    formatted_results = [{"status": row[0], "number_of_orders": row[1]} for row in results]
    return jsonify(formatted_results)

@app.route('/average_time_by_status')
def average_time_by_status():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT o.status, AVG(age(h.created_at, o.created_at)) AS average_duration 
        FROM smart_capital_customer.orders o 
        INNER JOIN smart_capital_customer.orders_histories h ON o.id = h.order_id 
        GROUP BY o.status;
    """)
    results = cur.fetchall()
    cur.close()
    conn.close()
    formatted_results = [{"status": row[0], "average_duration": str(row[1])} for row in results]
    return jsonify(formatted_results)

@app.route('/status_stability_duration')
def status_stability_duration():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH status_counts AS (
            SELECT
                status,
                COUNT(*) AS number_of_orders,
                MAX(created_at) AS last_status_change
            FROM
                smart_capital_customer.orders
            GROUP BY
                status
        )
        SELECT
            status,
            number_of_orders,
            NOW() - last_status_change AS stability_duration
        FROM
            status_counts;
    """)
    results = cur.fetchall()
    cur.close()
    conn.close()
    formatted_results = [{"status": row[0], "number_of_orders": row[1], "stability_duration": str(row[2])} for row in results]
    return jsonify(formatted_results)

@app.route('/time_since_last_change')
def time_since_last_change():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            status,
            MIN(created_at) AS oldest_status_change,
            NOW() - MIN(created_at) AS time_since_last_change
        FROM
            smart_capital_customer.orders_histories
        GROUP BY
            status;
    """)
    results = cur.fetchall()
    cur.close()
    conn.close()
    formatted_results = [{"status": row[0], "oldest_status_change": str(row[1]), "time_since_last_change": str(row[2])} for row in results]
    return jsonify(formatted_results)

@app.route('/clients_awaiting_credit_action')
def clients_awaiting_credit_action():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cur.execute("""
            SELECT 
                c.name AS client_name, 
                o.uuid AS order_uuid, 
                MAX(h.created_at) AS proposal_calculation_start_time,
                NOW() - MAX(h.created_at) AS time_since_proposal_calculation_started
            FROM 
                smart_capital_customer.orders o 
            INNER JOIN 
                smart_capital_customer.orders_histories h ON o.id = h.order_id
            INNER JOIN 
                smart_capital_customer.customers c ON o.customer_id = c.id
            WHERE 
                h.status = 'calculating_proposal' 
            GROUP BY 
                c.name, o.uuid order by proposal_calculation_start_time asc;
        """)
        results = cur.fetchall()
        cur.close()
        conn.close()
        formatted_results = [{
            "client_name": row['client_name'],
            "order_uuid": row['order_uuid'],
            "proposal_calculation_start_time": str(row['proposal_calculation_start_time']),
            "time_since_proposal_calculation_started": str(row['time_since_proposal_calculation_started'])
        } for row in results]
        return jsonify(formatted_results)
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
