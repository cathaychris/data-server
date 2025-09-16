import sqlite3
import time
import json
from flask import Flask, request, jsonify

# --- App and DB Setup ---
app = Flask(__name__)
DB_FILE = "/data/database.db" # Path inside the container

def get_db_connection():
    """Establishes a connection to the database."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Makes rows accessible by column name
    return conn

def init_db():
    """Initializes the database table if it doesn't exist."""
    conn = get_db_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY,
            data_filename TEXT NOT NULL,
            metadata_filename TEXT NOT NULL,
            extra_metadata JSON
        );
    """)
    conn.commit()
    conn.close()

# --- API Endpoints ---
@app.route('/record', methods=['POST'])
def add_record():
    """Adds a new record to the database."""
    data = request.get_json()

    # Basic validation
    if not data or 'data_filename' not in data or 'metadata_filename' not in data:
        return jsonify({"error": "Missing required fields"}), 400
    
    # Generate a unique ID
    record_id = int(time.time() * 1000)

    try:
        conn = get_db_connection()
        conn.execute(
            "INSERT INTO records (id, data_filename, metadata_filename, extra_metadata) VALUES (?, ?, ?, ?)",
            (
                record_id,
                data['data_filename'],
                data['metadata_filename'],
                json.dumps(data.get('extra_metadata', {})) # Safely get extra metadata
            )
        )
        conn.commit()
        conn.close()
        return jsonify({"success": True, "id": record_id}), 201
    except sqlite3.Error as e:
        return jsonify({"error": str(e)}), 500


@app.route('/record/<int:record_id>', methods=['GET'])
def get_record(record_id):
    """Retrieves a record by its unique ID."""
    try:
        conn = get_db_connection()
        record = conn.execute("SELECT * FROM records WHERE id = ?", (record_id,)).fetchone()
        conn.close()
        
        if record is None:
            return jsonify({"error": "Record not found"}), 404
        
        # Convert the row to a dictionary and parse the JSON field
        record_dict = dict(record)
        record_dict['extra_metadata'] = json.loads(record_dict['extra_metadata'])
        
        return jsonify(record_dict), 200
    except sqlite3.Error as e:
        return jsonify({"error": str(e)}), 500

# --- Main Execution ---
if __name__ == '__main__':
    init_db() # Create table on startup
    app.run(host='0.0.0.0', port=5000)