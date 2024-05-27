from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import os

app = Flask(__name__)

# Configuration
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('POSTGRES_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Define the Transaction model
class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    driver_id = db.Column(db.String(50), nullable=False)
    order_id = db.Column(db.String(50), nullable=False)
    order_amt = db.Column(db.Float, nullable=False)
    order_fee_amt = db.Column(db.Float, nullable=False)
    city_nm = db.Column(db.String(100), nullable=False)
    order_start_dttm = db.Column(db.DateTime, nullable=False)
    order_end_dttm = db.Column(db.DateTime, nullable=False)
    order_dt = db.Column(db.Date, nullable=False)

    def to_dict(self):
        return {
            "driver_id": self.driver_id,
            "order_id": self.order_id,
            "order_amt": self.order_amt,
            "order_fee_amt": self.order_fee_amt,
            "city_nm": self.city_nm,
            "order_start_dttm": self.order_start_dttm.isoformat(),
            "order_end_dttm": self.order_end_dttm.isoformat(),
            "order_dt": self.order_dt.isoformat()
        }

def validate_api_key(api_key):
    # Implement your API key validation logic here
    valid_api_keys = ["your-secure-api-key"]
    return api_key in valid_api_keys

@app.route('/transactions', methods=['GET'])
def get_transactions():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    transactions = Transaction.query.all()
    return jsonify([transaction.to_dict() for transaction in transactions]), 200

@app.route('/transactions', methods=['POST'])
def add_transaction():
    api_key = request.headers.get('API-Key')
    if not api_key or not validate_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        new_transaction = request.json
        transaction = Transaction(
            driver_id=new_transaction['driver_id'],
            order_id=new_transaction['order_id'],
            order_amt=new_transaction['order_amt'],
            order_fee_amt=new_transaction['order_fee_amt'],
            city_nm=new_transaction['city_nm'],
            order_start_dttm=datetime.fromisoformat(new_transaction['order_start_dttm']),
            order_end_dttm=datetime.fromisoformat(new_transaction['order_end_dttm']),
            order_dt=datetime.fromisoformat(new_transaction['order_dt']).date()
        )
        db.session.add(transaction)
        db.session.commit()

        return jsonify(transaction.to_dict()), 201
    except Exception as e:
        print(f"An error occurred: {e}")
        # Return OK response even if there's an error
        return jsonify({'message': 'Transaction received but there was an issue processing it.'}), 200

if __name__ == '__main__':
    app.run(debug=True)
