from flask import Flask, render_template, jsonify, request, Response
from statistics_queries import StatisticsQueries
import boto3
import pymysql
import json

app = Flask(__name__, template_folder='templates')

# ========== LECTURA DE PARÀMETRES SSM ==========
ssm = boto3.client('ssm', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

print("📝 Recuperant paràmetres de SSM Parameter Store...")

try:
    # DB HOST
    response_host = ssm.get_parameter(Name='/bdata-processing-server/env/DB_HOST', WithDecryption=False)
    DB_HOST = response_host['Parameter']['Value']

    # DB USER
    response_user = ssm.get_parameter(Name='/bdata-processing-server/env/DB_USER', WithDecryption=False)
    DB_USER = response_user['Parameter']['Value']

    # DB PASSWORD
    response_pass = ssm.get_parameter(Name='/bdata-processing-server/env/DB_PASS', WithDecryption=False)
    DB_PASS = response_pass['Parameter']['Value']

    # DB NAME
    response_db_name = ssm.get_parameter(Name='/bdata-processing-server/env/DB_NAME', WithDecryption=False)
    DB_NAME = response_db_name['Parameter']['Value']

    # BUCKET LOGS
    response_bucket = ssm.get_parameter(Name='/bdata-processing-server/env/BUCKET_LOGS', WithDecryption=False)
    BUCKET_LOGS = response_bucket['Parameter']['Value']

    # AWS REGION
    response_region = ssm.get_parameter(Name='/bdata-processing-server/env/AWS_REGION', WithDecryption=False)
    AWS_REGION = response_region['Parameter']['Value']

    print("✅ Paràmetres carregats correctament")
    print(f"   Host: {DB_HOST}")
    print(f"   BD: {DB_NAME}")
    print(f"   Bucket Logs: {BUCKET_LOGS}")
    print(f"   Regió: {AWS_REGION}")

except Exception as e:
    print(f"❌ Error recuperant paràmetres SSM: {e}")
    DB_HOST = None
    DB_USER = None
    DB_PASS = None
    DB_NAME = None
    BUCKET_LOGS = None
    AWS_REGION = None
    raise


# ========== FUNCIONS DE CONNEXIÓ ==========

def get_db_connection():
    """Obtenir connexió a la base de dades"""
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )


# ========== RUTES ==========

@app.route('/')
def dashboard():
    """Pàgina principal del panell de control"""
    return render_template('dashboard.html')


@app.route('/api/health')
def health():
    """Verificar connexió a BD"""
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({
            'status': 'ok',
            'message': 'Connectat a RDS',
            'database': DB_NAME,
            'host': DB_HOST
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/api/system-stats')
def system_stats():
    """Estadístiques generals del sistema"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Total d'execucions
            cursor.execute("SELECT COUNT(*) as total FROM etl_execution_summary")
            total = cursor.fetchone()['total']

            # Execucions exitoses
            cursor.execute("SELECT COUNT(*) as success FROM etl_execution_summary WHERE status='success'")
            success = cursor.fetchone()['success']

            # Execucions fallides
            cursor.execute("SELECT COUNT(*) as failed FROM etl_execution_summary WHERE status='failed'")
            failed = cursor.fetchone()['failed']

            # Total de registres processats
            cursor.execute("SELECT SUM(total_records) as records FROM etl_execution_summary WHERE status='success'")
            result = cursor.fetchone()
            records = result['records'] if result['records'] else 0

            # Duració mitjana
            cursor.execute(
                "SELECT AVG(duration_seconds) as avg_duration FROM etl_execution_summary WHERE status='success'")
            result = cursor.fetchone()
            avg_duration = result['avg_duration'] if result['avg_duration'] else 0

            # Última execució
            cursor.execute("SELECT MAX(started_at) as last_run FROM etl_execution_summary")
            result = cursor.fetchone()
            last_run = result['last_run']

        conn.close()

        success_rate = (success / total * 100) if total > 0 else 0

        return jsonify({
            'total_executions': total,
            'successful': success,
            'failed': failed,
            'total_records': int(records),
            'avg_duration': round(avg_duration, 2),
            'last_run': last_run.isoformat() if last_run else None,
            'success_rate': round(success_rate, 1)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/recent-executions')
def recent_executions():
    """Últimes execucions (últims 7 dies)"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT execution_id,
                                  process_date,
                                  status,
                                  started_at,
                                  completed_at,
                                  duration_seconds,
                                  total_records,
                                  error_message
                           FROM etl_execution_summary
                           WHERE started_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                           ORDER BY started_at DESC LIMIT 100
                           """)
            executions = cursor.fetchall()

        conn.close()

        # Formatar resultats
        result = []
        for ex in executions:
            result.append({
                'execution_id': ex['execution_id'],
                'process_date': ex['process_date'].isoformat() if ex['process_date'] else None,
                'status': ex['status'],
                'started_at': ex['started_at'].isoformat() if ex['started_at'] else None,
                'completed_at': ex['completed_at'].isoformat() if ex['completed_at'] else None,
                'duration_seconds': ex['duration_seconds'],
                'total_records': ex['total_records'],
                'error_message': ex['error_message'][:100] if ex['error_message'] else None
            })

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/executions-by-date/<date>')
def executions_by_date(date):
    """Execucions d'un dia específic"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT execution_id,
                                  status,
                                  started_at,
                                  completed_at,
                                  duration_seconds,
                                  total_records,
                                  error_message,
                                  s3_log_path
                           FROM etl_execution_summary
                           WHERE process_date = %s
                           ORDER BY started_at DESC
                           """, (date,))
            executions = cursor.fetchall()

        conn.close()

        result = []
        for ex in executions:
            result.append({
                'execution_id': ex['execution_id'],
                'status': ex['status'],
                'started_at': ex['started_at'].isoformat() if ex['started_at'] else None,
                'completed_at': ex['completed_at'].isoformat() if ex['completed_at'] else None,
                'duration_seconds': ex['duration_seconds'],
                'total_records': ex['total_records'],
                'error_message': ex['error_message'],
                's3_log_path': ex['s3_log_path']
            })

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/execution/<execution_id>')
def execution_details(execution_id):
    """Detalls d'una execució específica"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT execution_id,
                                  process_date,
                                  status,
                                  started_at,
                                  completed_at,
                                  duration_seconds,
                                  total_records,
                                  error_message,
                                  error_type,
                                  s3_log_path
                           FROM etl_execution_summary
                           WHERE execution_id = %s
                           """, (execution_id,))
            execution = cursor.fetchone()

        conn.close()

        if not execution:
            return jsonify({'error': 'No trobat'}), 404

        result = {
            'execution_id': execution['execution_id'],
            'process_date': execution['process_date'].isoformat() if execution['process_date'] else None,
            'status': execution['status'],
            'started_at': execution['started_at'].isoformat() if execution['started_at'] else None,
            'completed_at': execution['completed_at'].isoformat() if execution['completed_at'] else None,
            'duration_seconds': execution['duration_seconds'],
            'total_records': execution['total_records'],
            'error_message': execution['error_message'],
            'error_type': execution['error_type'],
            's3_log_path': execution['s3_log_path']
        }

        # Si hi ha log en S3, intentar traure-lo
        if execution['s3_log_path']:
            try:
                # Extreure bucket i key
                parts = execution['s3_log_path'].replace('s3://', '').split('/', 1)
                bucket = parts[0]
                key = parts[1]

                obj = s3.get_object(Bucket=bucket, Key=key)
                log_content = json.loads(obj['Body'].read().decode('utf-8'))
                result['s3_log'] = log_content
            except Exception as e:
                result['s3_log_error'] = f"No s'ha pogut llegir el log de S3: {str(e)}"

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/statistics')
def statistics():
    """Estadístiques per data"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT process_date,
                                  COUNT(*)                                            as total_executions,
                                  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                                  SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)  as failed,
                                  SUM(total_records)                                  as total_records,
                                  AVG(duration_seconds)                               as avg_duration
                           FROM etl_execution_summary
                           GROUP BY process_date
                           ORDER BY process_date DESC LIMIT 30
                           """)
            stats = cursor.fetchall()

        conn.close()

        result = []
        for stat in stats:
            result.append({
                'process_date': stat['process_date'].isoformat() if stat['process_date'] else None,
                'total_executions': stat['total_executions'],
                'successful': stat['successful'],
                'failed': stat['failed'],
                'total_records': stat['total_records'],
                'avg_duration': round(stat['avg_duration'], 2) if stat['avg_duration'] else 0
            })

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/download-log/<execution_id>')
def download_log(execution_id):
    """Descarregar log de S3"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT s3_log_path
                           FROM etl_execution_summary
                           WHERE execution_id = %s
                           """, (execution_id,))
            execution = cursor.fetchone()

        conn.close()

        if not execution or not execution['s3_log_path']:
            return jsonify({'error': 'No hi ha log disponible'}), 404

        s3_log_path = execution['s3_log_path']

        # Extreure bucket i key
        parts = s3_log_path.replace('s3://', '').split('/', 1)
        bucket = parts[0]
        key = parts[1]

        # Descarregar de S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        log_content = obj['Body'].read()

        # Retornar com a descàrrega
        from flask import send_file
        from io import BytesIO

        return send_file(
            BytesIO(log_content),
            mimetype='application/json',
            as_attachment=True,
            download_name=f"log-{execution_id}.json"
        )

    except Exception as e:
        return jsonify({'error': f'Error descargant: {str(e)}'}), 500


@app.route('/statistics')
def statistics():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, name_station
        FROM station;
        """)
    
    stations = cursor.fetchall()

    cursor.execute("""
        SELECT g.id as id, g.name_gas as name_gas, u.name_units as units FROM pollution_gas g INNER JOIN unit u ON g.id_unit = u.id
                   """)
    
    pollution_gas = cursor.fetchall()
    conn.close()
    cursor.close()
    return render_template(
        'statistics.html',
        stations = stations,
        pollution_gas = pollution_gas
    )


@app.route("/statistics/filter-results", methods=['POST'])
def execute_filters_statistics():
    data_to_filter = json.loads(request.form.get('data_to_filter'))

    start_date = data_to_filter["startDate"]
    end_date = data_to_filter["endDate"]
    pollution_gas = data_to_filter["pollutionGas"]
    stations = data_to_filter["stations"]

    conn = get_db_connection()
    cursor = conn.cursor()

    sq = StatisticsQueries(conn, cursor, start_date, end_date, pollution_gas, stations)

    has_data = sq.check_if_exists_data()

    avg_concentration_gas = []
    maximum_concentration_gas = []
    minimum_concentration_gas = []
    pollution_gas_info_per_day = []
    pollution_station_gas_info = []
    map_data = []
    boxplot_data = []

    if has_data:
        avg_concentration_gas = sq.execute_avg_concentration_gas()
        maximum_concentration_gas = sq.maximum_concentration_gas()
        minimum_concentration_gas = sq.minimum_concentration_gas()
        pollution_gas_info_per_day = sq.get_time_series_concentration_gas()
        pollution_station_gas_info = sq.get_data_to_compare_pollutions_per_station_type()
        map_data = sq.get_stations_latest_data()
        boxplot_data = sq.get_pollution_gas_concentration_boxplot_data()

    info = {
        'has_data': has_data,
        'stations': stations,
        'avg_concentration_gas': avg_concentration_gas,
        'maximum_concentration_gas': maximum_concentration_gas,
        'minimum_concentration_gas': minimum_concentration_gas,
        'pollution_gas_info_per_day': json.dumps(pollution_gas_info_per_day),
        'pollution_station_gas_info': json.dumps(pollution_station_gas_info),
        'map_data': json.dumps(map_data),
        'boxplot_data': json.dumps(boxplot_data)
    }

    html_content = render_template('statistics_data_graphs.html', info=info)

    conn.close()
    cursor.close()

    return Response(
        html_content,
        mimetype='text/html'
    )


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'No trobat'}), 404


@app.errorhandler(500)
def server_error(error):
    return jsonify({'error': 'Error intern del servidor'}), 500


# ========== MAIN ==========

if __name__ == '__main__':
    print("\nIniciant servidor Flask al port 80...")
    print(f"   URL: http://localhost:80")
    print(f"   Panell de control: http://localhost:80/\n")
    app.run(host='0.0.0.0', port=80, debug=False)