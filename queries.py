#sincronizza il contatore degli id con il valore massimo presente in tabella
QUERY_IMPOSTA_SEQUENZA = "SELECT setval('{sequenza}', COALESCE((SELECT MAX({colonna}) FROM {tabella}), 1))"

#recupera l'elenco alfabetico di tutte le corsie
QUERY_DIZIONARIO_CORSIE = "SELECT aisle_id, aisle FROM aisles ORDER BY aisle"
#recupera l'elenco alfabetico di tutti i reparti
QUERY_DIZIONARIO_REPARTI = "SELECT department_id, department FROM departments ORDER BY department"

#calcola le statistiche generali: ordini totali, prodotti, tasso riordino e frequenza acquisti
QUERY_METRICHE_PRINCIPALI = """
    SELECT
        (SELECT COUNT(DISTINCT order_id) FROM orders) AS numero_totale_ordini,
        (SELECT COUNT(*) FROM products) AS numero_totale_prodotti,
        (SELECT ROUND((100.0 * SUM(reordered) / COUNT(*))::numeric, 1)
        FROM order_details) AS percentuale_media_riordino,
        (SELECT ROUND(AVG(days_since_prior_order)::numeric, 1)
        FROM orders WHERE days_since_prior_order IS NOT NULL) AS media_giorni_tra_ordini
"""

#conta il numero di ordini effettuati per ogni ora del giorno
QUERY_ORDINI_PER_ORA = """
    SELECT order_hour_of_day AS ora_del_giorno, COUNT(*) AS totale_ordini
    FROM orders
    GROUP BY order_hour_of_day
    ORDER BY order_hour_of_day
"""

#conta il numero di ordini effettuati per ogni giorno della settimana
QUERY_ORDINI_PER_GIORNO = """
    SELECT order_dow AS giorno_della_settimana_numerico, COUNT(*) AS totale_ordini
    FROM orders
    GROUP BY order_dow
    ORDER BY order_dow
"""

#identifica i 10 reparti con il maggior volume di prodotti venduti
QUERY_TOP_REPARTI = """
    SELECT departments.department AS nome_reparto, COUNT(order_details.product_id) AS quantita_ordinata
    FROM order_details
    JOIN products ON order_details.product_id = products.product_id
    JOIN departments ON products.department_id = departments.department_id
    GROUP BY departments.department
    ORDER BY quantita_ordinata DESC
    LIMIT 10
"""

#calcola i 10 reparti con la più alta percentuale di prodotti riordinati
QUERY_TASSO_RIORDINO_REPARTI = """
    SELECT departments.department AS nome_reparto,
           ROUND(100.0 * SUM(order_details.reordered) / COUNT(*), 1) AS percentuale_di_riordino
    FROM order_details
    JOIN products ON order_details.product_id = products.product_id
    JOIN departments ON products.department_id = departments.department_id
    GROUP BY departments.department
    ORDER BY percentuale_di_riordino DESC
    LIMIT 10
"""

#cerca prodotti per nome e reparto, includendo il conteggio dei riordini in una fascia oraria
QUERY_RICERCA_PRODOTTI = """
    SELECT DISTINCT
        products.product_id AS "ID Prodotto",
        products.product_name AS "Nome Prodotto",
        aisles.aisle AS "Corsia",
        departments.department AS "Reparto",
        COALESCE(SUM(order_details.reordered) OVER (PARTITION BY products.product_id), 0) AS "Totale Riordini"
    FROM products
    JOIN aisles 
        ON products.aisle_id = aisles.aisle_id
    JOIN departments 
        ON products.department_id = departments.department_id
    LEFT JOIN order_details 
        ON products.product_id = order_details.product_id
    LEFT JOIN orders 
        ON order_details.order_id = orders.order_id
        AND orders.order_hour_of_day BETWEEN :parametro_ora_inizio AND :parametro_ora_fine
    WHERE products.product_name ILIKE :parametro_testo_ricerca
    {clausola_reparto}
    ORDER BY products.product_name
    LIMIT 500
"""

#inserisce una nuova corsia e restituisce l'id generato
QUERY_INSERISCI_CORSIA = "INSERT INTO aisles (aisle) VALUES (:parametro_nome_corsia) RETURNING aisle_id"
#cerca l'id di una corsia dato il suo nome
QUERY_VERIFICA_CORSIA = "SELECT aisle_id FROM aisles WHERE aisle = :parametro_nome_corsia"
#elenca tutte le corsie con i relativi id
QUERY_ELENCO_CORSIE = "SELECT aisle_id AS \"ID Corsia\", aisle AS \"Nome Corsia\" FROM aisles ORDER BY aisle"
#modifica il nome di una corsia esistente
QUERY_RINOMINA_CORSIA = "UPDATE aisles SET aisle = :parametro_nuovo_nome WHERE aisle_id = :parametro_id_corsia"
#conta quanti prodotti sono assegnati a una specifica corsia
QUERY_CONTA_PRODOTTI_CORSIA = "SELECT COUNT(*) AS totale_prodotti_collegati FROM products WHERE aisle_id = :parametro_id_corsia"
#elimina una corsia dal database
QUERY_ELIMINA_CORSIA = "DELETE FROM aisles WHERE aisle_id = :parametro_id_corsia"

#inserisce un nuovo reparto e restituisce l'id generato
QUERY_INSERISCI_REPARTO = "INSERT INTO departments (department) VALUES (:parametro_nome_reparto) RETURNING department_id"
#cerca l'id di un reparto dato il suo nome
QUERY_VERIFICA_REPARTO = "SELECT department_id FROM departments WHERE department = :parametro_nome_reparto"
#elenca tutti i reparti con i relativi id
QUERY_ELENCO_REPARTI = "SELECT department_id AS \"ID Reparto\", department AS \"Nome Reparto\" FROM departments ORDER BY department"
#modifica il nome di un reparto esistente
QUERY_RINOMINA_REPARTO = "UPDATE departments SET department = :parametro_nuovo_nome WHERE department_id = :parametro_id_reparto"
#conta quanti prodotti sono assegnati a uno specifico reparto
QUERY_CONTA_PRODOTTI_REPARTO = "SELECT COUNT(*) AS totale_prodotti_collegati FROM products WHERE department_id = :parametro_id_reparto"
#elimina un reparto dal database
QUERY_ELIMINA_REPARTO = "DELETE FROM departments WHERE department_id = :parametro_id_reparto"

#aggiunge un nuovo prodotto specificando nome, corsia e reparto
QUERY_INSERISCI_PRODOTTO = "INSERT INTO products (product_name, aisle_id, department_id) VALUES (:parametro_nome_prodotto, :parametro_id_corsia, :parametro_id_reparto)"
#trova le corsie che contengono prodotti di un determinato reparto
QUERY_CORSIE_PER_REPARTO = "SELECT DISTINCT aisles.aisle_id, aisles.aisle FROM aisles JOIN products ON products.aisle_id = aisles.aisle_id WHERE products.department_id = :parametro_id_reparto ORDER BY aisles.aisle"
#recupera i dettagli di un prodotto per permetterne la modifica
QUERY_RICERCA_MODIFICA_PRODOTTO = "SELECT products.product_id, products.product_name, products.aisle_id, products.department_id FROM products {stringa_where} ORDER BY products.product_name LIMIT 500"
#aggiorna i dati di un prodotto esistente
QUERY_AGGIORNA_PRODOTTO = "UPDATE products SET product_name=:parametro_nuovo_nome, aisle_id=:parametro_nuova_corsia, department_id=:parametro_nuovo_reparto WHERE product_id=:parametro_id_prodotto"
#rimuove un prodotto da tutti gli ordini in cui compare
QUERY_ELIMINA_ORDINI_PRODOTTO = "DELETE FROM order_details WHERE product_id = :parametro_id_prodotto"
#elimina definitivamente un prodotto dal catalogo
QUERY_ELIMINA_PRODOTTO = "DELETE FROM products WHERE product_id = :parametro_id_prodotto"