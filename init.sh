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