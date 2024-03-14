from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

# Database settings - hardcoded for testing purposes
DB_HOST = "datasources.postgres.database.azure.com"
DB_NAME = "db_product_manager_prd"
DB_USER = "a55dba"
DB_PASS = "Bzjlep2010#lima"

@app.route('/test_db_connection')
def test_db_connection():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
        conn.close()
        return jsonify({"message": "Successfully connected to the database!"})
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
