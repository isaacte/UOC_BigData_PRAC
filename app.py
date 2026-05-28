import boto3
import pymysql
import json
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, send_file, request, Response
import io

try:
    from statistics_queries import StatisticsQueries
except ImportError:
    print("⚠️ StatisticsQueries no disponible, algunes funcions estaràn limitades")
    StatisticsQueries = None

app = Flask(__name__, template_folder='templates')

params = {}


def load_ssm_parameters():
    global params
    print("📝 Recuperant paràmetres de SSM Parameter Store...")
    ssm = boto3.client('ssm', region_name='us-east-1')

    try:
        names = [
            '/bdata-processing-server/env/DB_HOST',
            '/bdata-processing-server/env/DB_USER',
            '/bdata-processing-server/env/DB_PASS',
            '/bdata-processing-server/env/DB_NAME',
            '/bdata-processing-server/env/BUCKET_LOGS',
            '/bdata-processing-server/env/AWS_REGION'
        ]
        response = ssm.get_parameters(Names=names, WithDecryption=True)
        for param in response['Parameters']:
            param_name = param['Name'].split('/')[-1]
            params[param_name] = param['Value']

        print("✅ Paràmetres carregats correctament")
        print(f"   Host: {params.get('DB_HOST')}")
        print(f"   BD: {params.get('DB_NAME')}")
        print(f"   Bucket Logs: {params.get('BUCKET_LOGS')}")
        print(f"   Regió: {params.get('AWS_REGION')}")
    except Exception as e:
        print(f"❌ Error recuperant paràmetres: {e}")
        raise


def get_db_connection():
    return pymysql.connect(
        host=params['DB_HOST'],
        user=params['DB_USER'],
        password=params['DB_PASS'],
        database=params['DB_NAME'],
        cursorclass=pymysql.cursors.DictCursor
    )


@app.route('/')
def index():
    return render_template('dashboard.html')


@app.route('/api/health')
def health():
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500


@app.route('/api/system-stats')
def system_stats():
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT COUNT(*)                                            as total_executions,
                                  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                                  SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)  as failed,
                                  SUM(total_records)                                  as total_records
                           FROM etl_execution_summary
                           """)
            stats = cursor.fetchone()
        conn.close()

        return jsonify({
            'total_executions': stats['total_executions'] or 0,
            'successful': stats['successful'] or 0,
            'failed': stats['failed'] or 0,
            'total_records': stats['total_records'] or 0
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/recent-executions')
def recent_executions():
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT execution_id,
                                  process_date,
                                  started_at,
                                  completed_at,
                                  status,
                                  total_records,
                                  duration_seconds,
                                  s3_log_path
                           FROM etl_execution_summary
                           ORDER BY started_at DESC LIMIT 20
                           """)
            executions = cursor.fetchall()
        conn.close()

        result = []
        for exec in executions:
            result.append({
                'execution_id': exec['execution_id'],
                'process_date': exec['process_date'].isoformat() if exec['process_date'] else None,
                'started_at': exec['started_at'].isoformat() if exec['started_at'] else None,
                'completed_at': exec['completed_at'].isoformat() if exec['completed_at'] else None,
                'status': exec['status'],
                'total_records': exec['total_records'],
                'duration_seconds': exec['duration_seconds'],
                's3_log_path': exec['s3_log_path']
            })

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/executions-by-date/<date_str>')
def executions_by_date(date_str):
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT execution_id,
                                  process_date,
                                  started_at,
                                  completed_at,
                                  status,
                                  total_records,
                                  duration_seconds
                           FROM etl_execution_summary
                           WHERE DATE (process_date) = %s
                           ORDER BY started_at DESC
                           """, (date_str,))
            executions = cursor.fetchall()
        conn.close()

        result = []
        for exec in executions:
            result.append({
                'execution_id': exec['execution_id'],
                'process_date': exec['process_date'].isoformat() if exec['process_date'] else None,
                'started_at': exec['started_at'].isoformat() if exec['started_at'] else None,
                'completed_at': exec['completed_at'].isoformat() if exec['completed_at'] else None,
                'status': exec['status'],
                'total_records': exec['total_records'],
                'duration_seconds': exec['duration_seconds']
            })

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/execution/<execution_id>')
def execution_details(execution_id):
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT execution_id,
                                  process_date,
                                  started_at,
                                  completed_at,
                                  status,
                                  total_records,
                                  duration_seconds,
                                  s3_log_path,
                                  error_message,
                                  error_type
                           FROM etl_execution_summary
                           WHERE execution_id = %s
                           """, (execution_id,))
            execution = cursor.fetchone()
        conn.close()

        if not execution:
            return jsonify({'error': 'Execució no trobada'}), 404

        return jsonify({
            'execution_id': execution['execution_id'],
            'process_date': execution['process_date'].isoformat() if execution['process_date'] else None,
            'started_at': execution['started_at'].isoformat() if execution['started_at'] else None,
            'completed_at': execution['completed_at'].isoformat() if execution['completed_at'] else None,
            'status': execution['status'],
            'total_records': execution['total_records'],
            'duration_seconds': execution['duration_seconds'],
            's3_log_path': execution['s3_log_path'],
            'error_message': execution['error_message'],
            'error_type': execution['error_type']
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/etl-stats')
def etl_stats():
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT
                               DATE (process_date) as date, COUNT (*) as count, SUM (CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful, SUM (CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed, SUM (total_records) as total_records, AVG (duration_seconds) as avg_duration
                           FROM etl_execution_summary
                           WHERE process_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                           GROUP BY DATE (process_date)
                           ORDER BY date DESC
                           """)
            stats = cursor.fetchall()
        conn.close()

        result = []
        for stat in stats:
            result.append({
                'date': stat['date'].isoformat() if stat['date'] else None,
                'count': stat['count'],
                'successful': stat['successful'],
                'failed': stat['failed'],
                'total_records': stat['total_records'],
                'avg_duration': float(stat['avg_duration']) if stat['avg_duration'] else 0
            })

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/statistics')
def statistics():
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, name_station FROM station")
            stations = cursor.fetchall()

            cursor.execute("""
                           SELECT g.id, g.name_gas, u.name_units
                           FROM pollution_gas g
                                    INNER JOIN unit u ON g.id_unit = u.id
                           """)
            pollution_gas = cursor.fetchall()
        conn.close()

        return render_template(
            'statistics.html',
            stations=stations,
            pollution_gas=pollution_gas
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/statistics/filter-results', methods=['POST'])
def filter_results_statistics():
    if not StatisticsQueries:
        return jsonify({'error': 'StatisticsQueries no disponible'}), 500

    try:
        data_to_filter = json.loads(request.form.get('data_to_filter', '{}'))

        start_date = data_to_filter.get('startDate')
        end_date = data_to_filter.get('endDate')
        pollution_gas = data_to_filter.get('pollutionGas', [])
        stations = data_to_filter.get('stations', [])

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

        cursor.close()
        conn.close()

        return Response(html_content, mimetype='text/html')
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/download-log/<execution_id>')
def download_log(execution_id):
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT s3_log_path
                           FROM etl_execution_summary
                           WHERE execution_id = %s
                           """, (execution_id,))
            result = cursor.fetchone()
        conn.close()

        if not result or not result['s3_log_path']:
            return jsonify({'error': 'Log no trobat'}), 404

        s3_path = result['s3_log_path']
        bucket = s3_path.split('/')[2]
        key = '/'.join(s3_path.split('/')[3:])

        s3 = boto3.client('s3', region_name=params.get('AWS_REGION', 'us-east-1'))

        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            log_content = response['Body'].read()

            return send_file(
                io.BytesIO(log_content),
                mimetype='application/json',
                as_attachment=True,
                download_name=f'log-{execution_id}.json'
            )
        except Exception as e:
            return jsonify({'error': f'Error descarregant de S3: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Ruta no trobada'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Error intern del servidor'}), 500


if __name__ == '__main__':
    load_ssm_parameters()
    app.run(host='0.0.0.0', port=8080, debug=False)