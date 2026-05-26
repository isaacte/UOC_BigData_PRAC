import boto3
import pymysql
import sys


def get_db_credentials():
    print("Recuperant credencials de Parameter Store...")
    ssm = boto3.client('ssm', region_name='us-east-1')
    try:
        db_host = ssm.get_parameter(Name='/bdata-processing-server/env/DB_HOST', WithDecryption=False)['Parameter'][
            'Value']
        db_user = ssm.get_parameter(Name='/bdata-processing-server/env/DB_USER', WithDecryption=False)['Parameter'][
            'Value']
        db_password = ssm.get_parameter(Name='/bdata-processing-server/env/DB_PASS', WithDecryption=False)['Parameter'][
            'Value']
        db_name = ssm.get_parameter(Name='/bdata-processing-server/env/DB_NAME', WithDecryption=False)['Parameter'][
            'Value']
        return db_host, db_user, db_password, db_name
    except Exception as e:
        print(f"Error recuperant les credencials: {e}")
        sys.exit(1)


def init_database():
    host, user, password, db_name = get_db_credentials()

    print(f"Connectant a la base de dades '{db_name}' a {host}...")
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
    except pymysql.MySQLError as e:
        print(f"Error de connexió a MySQL: {e}")
        sys.exit(1)

    print("Connexió establerta. Comprovant i creant taules si cal...")

    # Llista de consultes SQL per crear les taules (l'ordre importa per les Foreign Keys)
    taules_sql = [
        """
        CREATE TABLE IF NOT EXISTS global_variables
        (
            id
            INT
            PRIMARY
            KEY
            AUTO_INCREMENT,
            start_date
            DATE
            NOT
            NULL,
            end_date
            DATE
            NOT
            NULL,
            is_running
            TINYINT
        (
            1
        ) DEFAULT 1
            );
        """,
            """"
        CREATE TABLE IF NOT EXISTS batch_execution_log
        (
                                                           id INT PRIMARY KEY AUTO_INCREMENT,
                                                           start_date DATE,
                                                           end_date DATE,
                                                           id_status INT DEFAULT 1,
                                                           update_date_status DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                                           path_result_athena VARCHAR(255),
            log_s3 VARCHAR(255")