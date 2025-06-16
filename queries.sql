-- 1) Número de clientes por tipo
SELECT client_type,
       COUNT(*) AS num_clients
FROM bronze_clients
GROUP BY client_type
ORDER BY num_clients DESC;

-- 2) Ingresos totales por mes (de las transacciones bronze)
SELECT month_of_year,
       SUM(total_price) AS total_revenue
FROM bronze_transactions
GROUP BY month_of_year
ORDER BY month_of_year;

-- 3) Top 5 clientes por gasto total (silver)
SELECT client_id,
       SUM(total_price) AS total_spent
FROM silver_transactions_enriched
GROUP BY client_id
ORDER BY total_spent DESC
LIMIT 5;

-- 4) Ingresos y número de transacciones por región y categoría de riesgo (silver)
SELECT region,
       risk_category,
       COUNT(*)    AS num_tx,
       SUM(total_price) AS revenue
FROM silver_transactions_enriched
GROUP BY region, risk_category
ORDER BY region, risk_category;

-- 5) Resumen de riesgo de proveedores: cuántos proveedores en cada categoría (bronze)
SELECT risk_category,
       COUNT(*) AS num_providers
FROM (
  SELECT *,
         CASE 
           WHEN risk_score < 40 THEN 'Low'
           WHEN risk_score < 70 THEN 'Medium'
           ELSE 'High'
         END AS risk_category
  FROM bronze_providers
) t
GROUP BY risk_category;

-- 6) KPIs Gold: ingresos por región (gold)
SELECT * 
FROM gold_revenue_by_region
ORDER BY total_revenue DESC;
