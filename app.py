from flask import Flask, render_template, jsonify
import boto3
import pymysql
import json

app = Flask(__name__, template_folder='templates')

# ========== LECTURA DE PARÁMETROS SSM ==========
ssm = boto3.client('ssm', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

print("📝 Recuperando parámetros de SSM Parameter Store...")

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

    print("✅ Parámetros cargados correctamente")
    print(f"   Host: {DB_HOST}")
    print(f"   BD: {DB_NAME}")
    print(f"   Bucket Logs: {BUCKET_LOGS}")
    print(f"   Región: {AWS_REGION}")

except Exception as e:
    print(f"❌ Error recuperando parámetros SSM: {e}")
    DB_HOST = None
    DB_USER = None
    DB_PASS = None
    DB_NAME = None
    BUCKET_LOGS = None
    AWS_REGION = None
    raise


# ========== FUNCIONES DE CONEXIÓN ==========

def get_db_connection():
    """Obtener conexión a la base de datos"""
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )


# ========== RUTAS ==========

@app.route('/')
def dashboard():
    """Página principal del dashboard"""
    return render_template('dashboard.html')


@app.route('/api/health')
def health():
    """Verificar conexión a BD"""
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({
            'status': 'ok',
            'message': 'Conectado a RDS',
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
    """Estadísticas generales del sistema"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Total de ejecuciones
            cursor.execute("SELECT COUNT(*) as total FROM etl_execution_summary")
            total = cursor.fetchone()['total']

            # Ejecuciones exitosas
            cursor.execute("SELECT COUNT(*) as success FROM etl_execution_summary WHERE status='success'")
            success = cursor.fetchone()['success']

            # Ejecuciones fallidas
            cursor.execute("SELECT COUNT(*) as failed FROM etl_execution_summary WHERE status='failed'")
            failed = cursor.fetchone()['failed']

            # Total de registros procesados
            cursor.execute("SELECT SUM(total_records) as records FROM etl_execution_summary WHERE status='success'")
            result = cursor.fetchone()
            records = result['records'] if result['records'] else 0

            # Duración promedio
            cursor.execute(
                "SELECT AVG(duration_seconds) as avg_duration FROM etl_execution_summary WHERE status='success'")
            result = cursor.fetchone()
            avg_duration = result['avg_duration'] if result['avg_duration'] else 0

            # Última ejecución
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
    """Últimas ejecuciones (últimos 7 días)"""
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

        # Formatear resultados
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
    """Ejecuciones de un día específico"""
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
    """Detalles de una ejecución específica"""
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
            return jsonify({'error': 'No encontrado'}), 404

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

        # Si hay log en S3, intentar traerlo
        if execution['s3_log_path']:
            try:
                # Extraer bucket y key
                parts = execution['s3_log_path'].replace('s3://', '').split('/', 1)
                bucket = parts[0]
                key = parts[1]

                obj = s3.get_object(Bucket=bucket, Key=key)
                log_content = json.loads(obj['Body'].read().decode('utf-8'))
                result['s3_log'] = log_content
            except Exception as e:
                result['s3_log_error'] = f"No se pudo leer log de S3: {str(e)}"

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/statistics')
def statistics():
    """Estadísticas por fecha"""
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


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'No encontrado'}), 404


@app.errorhandler(500)
def server_error(error):
    return jsonify({'error': 'Error interno del servidor'}), 500


# ========== MAIN ==========

if __name__ == '__main__':
    print("\n🚀 Iniciando servidor Flask en puerto 8080...")
    print(f"   URL: http://localhost:8080")
    print(f"   Dashboard: http://localhost:8080/\n")
    app.run(host='0.0.0.0', port=8080, debug=False)