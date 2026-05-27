#!/bin/bash
# 1. Actualitzem i instal·lem dependències
sudo dnf update -y
sudo dnf install git python3-pip -y

# 2. Clonar el repositori
cd /home/ec2-user
git clone https://github.com/isaacte/UOC_BigData_PRAC app_code
cd app_code

# 3. Instal·lar llibreries de python
pip3 install -r requirements.txt

# 4. Executar l'script de creació de taules
python3 init_db.py

# 5. Configurar el servei web (systemd)
sudo cp flaskapp.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable flaskapp
sudo systemctl start flaskapp

# 6.Configurar el cron de l'ETL
echo "0 3 * * * ec2-user /usr/bin/python3 /home/ec2-user/app_code/etl.py >> /home/ec2-user/etl_cron.log 2>&1" | sudo tee /etc/cron.d/etl_bdata
sudo chmod 0644 /etc/cron.d/etl_bdata