import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

def create_database():
    conn = None
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DATABASE, 
            user=USER,
            password=PASSWORD
        )
        cursor = conn.cursor()

        sql_script = """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS departments (
            department_id INTEGER PRIMARY KEY,
            department VARCHAR(255) NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS aisles (
            aisle_id INTEGER PRIMARY KEY,
            aisle VARCHAR(255) NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            aisle_id INTEGER NOT NULL,
            department_id INTEGER NOT NULL,
            FOREIGN KEY (aisle_id) REFERENCES aisles (aisle_id) ON DELETE CASCADE,
            FOREIGN KEY (department_id) REFERENCES departments (department_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            eval_set VARCHAR(50) DEFAULT 'train',
            order_number INTEGER CHECK(order_number > 0),
            order_dow INTEGER CHECK(order_dow >= 0 AND order_dow <= 6),
            order_hour_of_day INTEGER CHECK(order_hour_of_day >= 0 AND order_hour_of_day <= 23),
            days_since_prior_order FLOAT CHECK(days_since_prior_order >= 0),
            FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS order_details (
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            add_to_cart_order INTEGER NOT NULL CHECK(add_to_cart_order > 0),
            reordered INTEGER DEFAULT 0 CHECK(reordered IN (0, 1)),
            PRIMARY KEY (order_id, product_id),
            FOREIGN KEY (order_id) REFERENCES orders (order_id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products (product_id) ON DELETE CASCADE
        );
        """

        cursor.execute(sql_script)
        
        conn.commit()
        print("db creato con successo!")

    except Exception as errore:
        print(f"errore durante la creazione del database: {errore}")
    
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_database()