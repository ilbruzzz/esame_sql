import sqlite3

def create_database():
    conn = sqlite3.connect('instacart.db')
    cursor = conn.cursor()

    #normalizzazione del csv in formato 3NF guarda ER.jpg per il diagramma dettagliato
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

    cursor.executescript(sql_script)
    conn.commit()
    conn.close()
    print("database creato")

#test
if __name__ == "__main__":
    create_database()