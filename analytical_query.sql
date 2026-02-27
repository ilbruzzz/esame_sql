--il primo blocco crea un riassunto delle vendite generali per ogni singolo reparto
    --conta le vendite
    --calcola il percentuale di riordino
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
        d.department_id,
        d.department AS nome_reparto,
        COUNT(od.product_id) AS totale_prodotti_venduti,
        ROUND(CAST(SUM(od.reordered) AS FLOAT) / COUNT(od.product_id) * 100, 2) AS percentuale_riordino
    FROM departments d
    JOIN products p ON d.department_id = p.department_id
    JOIN order_details od ON p.product_id = od.product_id
    GROUP BY d.department_id, d.department
),
OreDiPunta AS (
    SELECT 
        d.department_id,
        o.order_hour_of_day,
        COUNT(*) AS ordini_per_ora,
        ROW_NUMBER() OVER(PARTITION BY d.department_id ORDER BY COUNT(*) DESC) AS num_riga
    FROM departments d
    JOIN products p ON d.department_id = p.department_id
    JOIN order_details od ON p.product_id = od.product_id
    JOIN orders o ON od.order_id = o.order_id
    GROUP BY d.department_id, o.order_hour_of_day
)
SELECT 
    sr.nome_reparto,
    sr.totale_prodotti_venduti,
    sr.percentuale_riordino,
    op.order_hour_of_day AS ora_di_punta_vendite
FROM StatisticheReparti sr
JOIN OreDiPunta op ON sr.department_id = op.department_id AND op.num_riga = 1
ORDER BY sr.totale_prodotti_venduti DESC;