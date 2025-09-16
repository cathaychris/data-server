import sqlite3
import time
import json
from flask import Flask, request, jsonify

app = Flask(__name__)
DB_FILE = "/data/database.db"

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY,
                data_filename TEXT NOT NULL,
                metadata_filename TEXT NOT NULL,
                extra_metadata JSON
            );
        """)
        conn.commit()

# --- NEW SEARCH ENDPOINT ---
@app.route('/records/search', methods=['GET'])
def search_records():
    """
    Searches records by timestamp range and/or metadata values.
    Example URLS:
    /records/search?start_id=1726517425123&end_id=1726517430000
    /records/search?user=alex
    /records/search?is_processed=false&source=sensor-A
    """
    query_params = request.args
    
    # Start building the query dynamically and safely
    base_query = "SELECT * FROM records"
    conditions = []
    params = []

    # Handle timestamp range search
    if 'start_id' in query_params:
        conditions.append("id >= ?")
        params.append(query_params['start_id'])
    
    if 'end_id' in query_params:
        conditions.append("id <= ?")
        params.append(query_params['end_id'])

    # Handle dynamic metadata filtering
    for key, value in query_params.items():
        if key not in ['start_id', 'end_id']:
            # Use json_extract to query inside the JSON column
            conditions.append("json_extract(extra_metadata, '$.' || ?) = ?")
            params.extend([key, value])

    if not conditions:
        return jsonify({"error": "No search parameters provided"}), 400

    # Combine all conditions into the final query
    final_query = f"{base_query} WHERE {' AND '.join(conditions)}"

    try:
        with get_db_connection() as conn:
            records = conn.execute(final_query, params).fetchall()
        
        # Convert rows to a list of dictionaries
        results = []
        for record in records:
            record_dict = dict(record)
            if record_dict['extra_metadata']:
                record_dict['extra_metadata'] = json.loads(record_dict['extra_metadata'])
            results.append(record_dict)
            
        return jsonify(results), 200
    except sqlite3.Error as e:
        return jsonify({"error": str(e)}), 500


# --- EXISTING ENDPOINTS (no changes needed) ---
@app.route('/record', methods=['POST'])
def add_record():
    data = request.get_json()
    if not data or 'data_filename' not in data or 'metadata_filename' not in data:
        return jsonify({"error": "Missing required fields"}), 400
    
    record_id = int(time.time() * 1000)
    try:
        with get_db_connection() as conn:
            conn.execute(
                "INSERT INTO records (id, data_filename, metadata_filename, extra_metadata) VALUES (?, ?, ?, ?)",
                (
                    record_id,
                    data['data_filename'],
                    data['metadata_filename'],
                    json.dumps(data.get('extra_metadata', {}))
                )
            )
            conn.commit()
        return jsonify({"success": True, "id": record_id}), 201
    except sqlite3.Error as e:
        return jsonify({"error": str(e)}), 500

@app.route('/record/<int:record_id>', methods=['GET'])
def get_record(record_id):
    try:
        with get_db_connection() as conn:
            record = conn.execute("SELECT * FROM records WHERE id = ?", (record_id,)).fetchone()
        
        if record is None:
            return jsonify({"error": "Record not found"}), 404
        
        record_dict = dict(record)
        if record_dict['extra_metadata']:
            record_dict['extra_metadata'] = json.loads(record_dict['extra_metadata'])
        return jsonify(record_dict), 200
    except sqlite3.Error as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)