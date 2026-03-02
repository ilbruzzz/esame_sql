from create_table import create_database
from load import load_data
import sys
import subprocess

#richiamo le funzioni per la creazione del db e caricamento dati
create_database()
load_data()

#esegui streamlit nel app.py
subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py"])