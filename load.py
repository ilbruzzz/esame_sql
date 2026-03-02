import pandas as pd
import sqlite3

def load_data():
    csv_file = 'instacart.csv'
    db_file = 'instacart.db'
    
    #leggiamo il csv
    df = pd.read_csv(csv_file)
    
    #pulizia dati
    #rimuoviamo duplicate (per intero) o con valori mancanti per garantire 
    #l'integrità dei dati grezzi prima della scomposizione.
    df = df.drop_duplicates()
    df = df.dropna()
    

    #trasformiamo il file CSV (denormalizzato) in tabelle relazionali (3NF).
    #usiamo drop_duplicates(subset=[...]) sulle chiavi primarie per estrarre 
    #solo le entità uniche (es. un singolo record per ogni prodotto) ed evitare 
    # la duplicazione delle primary key nel database.

    users = df[['user_id']].drop_duplicates()
    departments = df[['department_id',
                        'department']].drop_duplicates(subset=['department_id'])
    aisles = df[['aisle_id',
                    'aisle']].drop_duplicates(subset=['aisle_id'])
    products = df[['product_id',
                    'product_name',
                    'aisle_id',
                    'department_id']].drop_duplicates()
    orders = df[['order_id',
                    'user_id',
                    'eval_set',
                    'order_number',
                    'order_dow',
                    'order_hour_of_day',
                    'days_since_prior_order']].drop_duplicates()
    order_details = df[['order_id',
                        'product_id',
                        'add_to_cart_order',
                        'reordered']].drop_duplicates()

    #connessione al db
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    #svuotiamo le tabelle esistenti prima del caricamento. 
    #questo ci permette di eseguirlo 
    #più volte senza rischiare di duplicare i dati nel database.
    try:
        cursor.execute("DELETE FROM order_details;")
        cursor.execute("DELETE FROM orders;")
        cursor.execute("DELETE FROM products;")
        cursor.execute("DELETE FROM aisles;")
        cursor.execute("DELETE FROM departments;")
        cursor.execute("DELETE FROM users;")
        conn.commit()

        #caricamento dei dati nelle tabelle corrispondenti
        users.to_sql('users', conn, if_exists='append', index=False)
        departments.to_sql('departments', conn, if_exists='append', index=False)
        aisles.to_sql('aisles', conn, if_exists='append', index=False)
        products.to_sql('products', conn, if_exists='append', index=False)
        orders.to_sql('orders', conn, if_exists='append', index=False)
        order_details.to_sql('order_details', conn, if_exists='append', index=False)
        
        print("caricamento completato")
        
    except Exception as errore:
        print(repr(errore))
        
    finally:
        conn.close()

#test funzione
if __name__ == "__main__":
    load_data()