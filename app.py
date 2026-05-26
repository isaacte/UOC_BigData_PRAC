from flask import Flask, render_template_string
import boto3
import pymysql

app = Flask(__name__)

# 1. Recuperem les credencials de la BBDD de forma segura des de Parameter Store
ssm = boto3.client('ssm', region_name='us-east-1')

try:
    resposta_host = ssm.get_parameter(Name='/bdata-processing-server/env/DB_HOST', WithDecryption=False)
    DB_HOST = resposta_host['Parameter']['Value']

    resposta_pass = ssm.get_parameter(Name='/bdata-processing-server/env/DB_PASS', WithDecryption=True)
    DB_PASSWORD = resposta_pass['Parameter']['Value']

    print("🔐 [OK] Credencials del Parameter Store carregades correctament.")
except Exception as e:
    print(f"❌ [ERROR] No s'han pogut carregar els paràmetres de SSM: {e}")
    DB_HOST = None
    DB_PASSWORD = None
# 2. Definim la ruta principal de la pàgina web (El nostre Hello World)
@app.route('/')
def home():
    # Plantilla HTML senzilla per fer-ho visible i maco al navegador
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Control de Qualitat de l'Aire</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 50px; background-color: #f4f7f6; color: #333; }
            .container { background-color: white; padding: 30px; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.05); max-width: 600px; margin: 0 auto; border-top: 5px solid #2ecc71; }
            h1 { color: #2c3e50; }
            .status { background-color: #e8f8f5; color: #2ecc71; padding: 5px 10px; border-radius: 5px; font-weight: bold; display: inline-block; }
            .info { color: #7f8c8d; font-size: 0.9em; margin-top: 20px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Hello World - Panell de Dades de l'Aire</h1>
            <p>La instància d'EC2 està corrent correctament i és pública de cara a internet.</p>
            <p>Estat de la connexió segura amb Parameter Store: <span class="status">CONNECTED</span></p>

            <div class="info">
                <p>📍 <strong>Endpoint RDS connectat:</strong> {{ rds_host }}</p>
                <p>🚀 <em>Proper pas: Executar l'script de processament i omplir les dades reals a MySQL!</em></p>
            </div>
        </div>
    </body>
    </html>
    """
    # Passem el paràmetre de l'endpoint a la web només per confirmar que s'ha llegit bé
    return render_template_string(html_template, rds_host=DB_HOST if DB_HOST else "Error de càrrega")


# 3. Arrenquem el servidor web al port 8080 accessible des de fora
if __name__ == '__main__':
    print("🚀 Aixecant el servidor Flask al port 8080...")
    app.run(host='0.0.0.0', port=8080)
