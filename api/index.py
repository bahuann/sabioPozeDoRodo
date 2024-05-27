from flask import Flask, request, jsonify
import json

app = Flask(__name__)

# Example data store (initially load from file if exists)
try:
    with open('transactions.json', 'r') as file:
        transactions = json.load(file)
except FileNotFoundError:
    transactions = []

def validate_api_key(api_key):
    # Implement your API key validation logic here
    valid_api_keys = ["123123123"]
    return api_key in valid_api_keys

@app.route('/transactions', methods=['GET'])
def get_transactions():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    return jsonify(transactions), 200

@app.route('/transactions', methods=['POST'])
def add_transaction():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        new_transaction = request.json
        transactions.append(new_transaction)
        
        # Write transactions to file
        with open('transactions.json', 'w') as file:
            json.dump(transactions, file, indent=4)
        
        return jsonify(new_transaction), 201
    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500

if __name__ == '__main__':
    app.run(debug=True)

if __name__ == '__main__':
    app.run(debug=True)
