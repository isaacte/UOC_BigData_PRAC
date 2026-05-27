from flask import Flask, render_template, request, Response
import boto3
import pymysql

app = Flask(__name__, template_folder='.')

ssm = boto3.client('ssm', region_name='us-east-1')
s3 = boto3.client('s3')

try:
    resposta_host = ssm.get_parameter(Name='/bdata-processing-server/env/DB_HOST', WithDecryption=False)
    db_host = resposta_host['Parameter']['Value']

    resposta_pass = ssm.get_parameter(Name='/bdata-processing-server/env/DB_PASS', WithDecryption=False)
    db_password = resposta_pass['Parameter']['Value']

    resposta_user = ssm.get_parameter(Name='/bdata-processing-server/env/DB_USER', WithDecryption=False)
    db_user = resposta_user['Parameter']['Value']

    resposta_db_name = ssm.get_parameter(Name='/bdata-processing-server/env/DB_NAME', WithDecryption=False)
    db_name = resposta_db_name['Parameter']['Value']
    
    resposta_region_aws = ssm.get_parameter(Name='/bdata-processing-server/env/AWS_REGION', WithDecryption=False)
    aws_region = resposta_region_aws['Parameter']['Value']
except Exception as e:
    print(f"Error al recuperar les credencials: {e}")
    db_host = None
    db_password = None
    db_user = None
    db_name = None
    aws_region = None


DB_HOST = db_host
DB_USER = db_user
DB_PASS = db_password
DB_NAME = db_name
AWS_REGION = aws_region
LIMIT = 2

def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

# 2. Definim la ruta principal de la pàgina web
@app.route('/')
def home():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, execution_date, update_date_status,
        start_date, end_date, total_pollution_concentrations_added,
        id_status, log_s3
        FROM batch_execution_log;
        """)
    batch_executions = cursor.fetchall()

    cursor.execute("""
        SELECT start_date, end_date, date_last_update, is_running
        FROM global_variables where id = 1;
        """)
    global_vars = cursor.fetchone()

    if global_vars['is_running']:
        is_running = 'fa fa-check'
        color = 'green'
    else:
        is_running = 'fa fa-times'
        color = 'red'

    total_records_added = 0
    global_vars_formatted = {
        'start_date': global_vars['start_date'].strftime('%Y-%m-%d %H:%M:%S'),
        'end_date': global_vars['end_date'].strftime('%Y-%m-%d %H:%M:%S'),
        'date_last_update': global_vars['date_last_update'].strftime('%Y-%m-%d %H:%M:%S'),
        'is_running': {'icon': is_running, 'color': color},
        'total_records_added': total_records_added
    }

    data_to_render = []

    

    for batch_execution in batch_executions:

        color = ''
        if batch_execution['id_status'] == 1:
            status = 'fa fa-calendar-check-o'
            text = 'Programat'
        elif batch_execution['id_status'] == 2:
            status = 'fa fa-spinner fa-spin'
            text = 'Preparant-se per carregar info del CSV'
        elif batch_execution['id_status'] == 3:
            status = 'fa fa-spinner fa-spin'
            text = 'Emmagatzemant la informació a la base de dades'
        elif batch_execution['id_status'] == 4:
            status = 'fa fa-check'
            text = 'Finalitzat correctament'
            color = 'green'
        else:
            status = 'fa fa-times'
            text = 'Error'
            color = 'red'

        total_records_added += batch_execution['total_pollution_concentrations_added']

        data_to_render.append({
            'id': batch_execution['id'],
            'execution_date': batch_execution['execution_date'].strftime('%Y-%m-%d %H:%M:%S'),
            'update_date_status': batch_execution['update_date_status'].strftime('%Y-%m-%d %H:%M:%S'),
            'start_date_batch': batch_execution['start_date'].strftime('%Y-%m-%d %H:%M:%S'),
            'end_date_batch': batch_execution['end_date'].strftime('%Y-%m-%d %H:%M:%S'),
            'total_records_pollution_added': batch_execution['total_pollution_concentrations_added'],
            'status': {'icon': status, 'text': text, 'color': color},
            'log_s3': batch_execution['log_s3']
        })
    
    conn.close()
    cursor.close()
    global_vars_formatted['total_records_added'] = total_records_added
    return render_template(
        'index.html',
        batch_executions=data_to_render,
        global_vars=global_vars_formatted,

    )

@app.route('/download-log', methods=['POST'])
def download_log():
    key_file = request.form.get('log_url')
    batch_execution = request.form.get('batch_execution_id')
    
    if not key_file:
        return "Error: No s'ha rebut la ruta del log", 400
    
    bucket_name = 'bdata-etl-logs'

    try:
        s3_object = s3.get_object(Bucket=bucket_name, Key=key_file)
        
        # Llegim el contingut en cru (bytes o text)
        log_content = s3_object['Body'].read()

        # 3. Retornem la resposta amb el contingut del fitxer
        return Response(
            log_content,
            mimetype='text/plain',
            headers={
                "Content-Disposition": f"attachment; filename=log-file{batch_execution}"
            }
        )

    except Exception as e:
        return f"Error en descarregar el log: {str(e)}", 500


# 3. Arrenquem el servidor web al port 8080 accessible des de fora
if __name__ == '__main__':
    print("🚀 Aixecant el servidor Flask al port 8080...")
    app.run(host='0.0.0.0', port=8080)