from flask import Flask, request, jsonify

app = Flask(__name__)

# Example data store
transactions = [
    {
        "driver_id": "D123",
        "order_id": "O456",
        "order_amt": 100.0,
        "order_fee_amt": 5.0,
        "city_nm": "New York",
        "order_start_dttm": "2023-01-01T10:00:00",
        "order_end_dttm": "2023-01-01T10:30:00",
        "order_dt": "2023-01-01"
    },
    # Add more transactions as needed
]

def validate_api_key(api_key):
    # Implement your API key validation logic here
    valid_api_keys = ["your-secure-api-key"]
    return api_key in valid_api_keys

@app.route('/transactions', methods=['GET'])
def get_transactions():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    return jsonify(transactions), 200

if __name__ == '__main__':
    app.run()
