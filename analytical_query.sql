--il primo blocco crea un riassunto delle vendite generali per ogni singolo reparto
    --conta le vendite
    --calcola la percentuale di riordino
-- il secondo blocco serve per capire quando le persone comprano di più.
    --raggruppa le vendite per ora
    --crea una classifica
--nel terzo blocco si fondono i dati
    --prende il riassunto delle vendite dal primo blocco
    --incastriamo i dati con la classifica delle vendite
        --filtriamo la classifica prendendo solo il numero 1 (l'orario con più vendite).
    --ordina i risultati dal reparto che ha venduto di più a quello che ha venduto di meno

WITH StatisticheReparti AS (
    SELECT 
        departments.department_id,
        departments.department AS nome_reparto,
        COUNT(order_details.product_id) AS totale_prodotti_venduti,
        ROUND(CAST(SUM(order_details.reordered) AS FLOAT) / COUNT(order_details.product_id) * 100, 2) AS percentuale_riordino
    FROM departments
    JOIN products ON departments.department_id = products.department_id
    JOIN order_details ON products.product_id = order_details.product_id
    GROUP BY departments.department_id, departments.department
),
OreDiPunta AS (
    SELECT 
        departments.department_id,
        orders.order_hour_of_day,
        COUNT(*) AS ordini_per_ora,
        ROW_NUMBER() OVER(PARTITION BY departments.department_id ORDER BY COUNT(*) DESC) AS num_riga
    FROM departments
    JOIN products ON departments.department_id = products.department_id
    JOIN order_details ON products.product_id = order_details.product_id
    JOIN orders ON order_details.order_id = orders.order_id
    GROUP BY departments.department_id, orders.order_hour_of_day
)
SELECT 
    StatisticheReparti.nome_reparto,
    StatisticheReparti.totale_prodotti_venduti,
    StatisticheReparti.percentuale_riordino,
    OreDiPunta.order_hour_of_day AS ora_di_punta_vendite
FROM StatisticheReparti
JOIN OreDiPunta ON StatisticheReparti.department_id = OreDiPunta.department_id AND OreDiPunta.num_riga = 1
ORDER BY StatisticheReparti.totale_prodotti_venduti DESC;