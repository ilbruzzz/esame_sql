import pandas as pd
import psycopg2
import os

#.env
from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

COLLEGAMENTO_DB_APP_PY =f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{DATABASE}"

csv = "instacart.csv"
cartella_corrente = os.path.dirname(os.path.abspath(__file__))
file = os.path.join(cartella_corrente, csv)

def load_csv():
    
    # leggiamo il csv
    df = pd.read_csv(file)
    
    # pulizia dati
    #rimuoviamo duplicate (per intero) o con valori mancanti per garantire 
    #l'integrità dei dati grezzi prima della scomposizione.
    df = df.drop_duplicates()
    df = df.dropna()
    
    #trasformiamo il file CSV (denormalizzato) in tabelle relazionali (3NF).
    #usiamo drop_duplicates(subset=[...]) sulle chiavi primarie per estrarre 
    #solo le entità uniche (es. un singolo record per ogni prodotto) ed evitare 
    #la duplicazione delle primary key nel database.
    users = df[["user_id"]].drop_duplicates()
    departments = df[["department_id", "department"]].drop_duplicates(subset=["department_id"])
    aisles = df[["aisle_id", "aisle"]].drop_duplicates(subset=["aisle_id"])
    products = df[["product_id", "product_name", "aisle_id", "department_id"]].drop_duplicates()
    orders = df[["order_id", "user_id", "eval_set", "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order"]].drop_duplicates()
    order_details = df[["order_id", "product_id", "add_to_cart_order", "reordered"]].drop_duplicates()

    conn = None
    try:
        #connessione al db su Docker
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DATABASE, 
            user=USER,
            password=PASSWORD
        )
        cursor = conn.cursor()
        
        #svuotiamo le tabelle esistenti prima del caricamento. 
        #questo ci permette di eseguirlo più volte senza rischiare di duplicare i dati nel database.
        cursor.execute("TRUNCATE TABLE order_details, orders, products, aisles, departments, users CASCADE;")
        
        #caricamento dei dati nelle tabelle corrispondenti trasformando i df in liste
        cursor.executemany("INSERT INTO users (user_id) VALUES (%s)", 
                            users.values.tolist())
        
        cursor.executemany("INSERT INTO departments (department_id, department) VALUES (%s, %s)", 
                            departments.values.tolist())
        
        cursor.executemany("INSERT INTO aisles (aisle_id, aisle) VALUES (%s, %s)", 
                            aisles.values.tolist())
        
        cursor.executemany("INSERT INTO products (product_id, product_name, aisle_id, department_id) VALUES (%s, %s, %s, %s)", 
                            products.values.tolist())
        
        cursor.executemany("INSERT INTO orders (order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order) VALUES (%s, %s, %s, %s, %s, %s, %s)", 
                            orders.values.tolist())
        
        cursor.executemany("INSERT INTO order_details (order_id, product_id, add_to_cart_order, reordered) VALUES (%s, %s, %s, %s)", 
                            order_details.values.tolist())
        
        conn.commit()
        print(f"'{csv}' caricato correttamente")
        
    except Exception as errore:
        if conn:
            conn.rollback()
        print(errore)
        
    finally:
        if conn:
            cursor.close()
            conn.close()

#test funzione
if __name__ == "__main__":
    load_csv()