-- Faktor demografi → misalnya, apakah usia muda lebih berisiko default?

WITH client_default AS (
    SELECT 
        c.client_id,
        c.age,
        CASE 
            WHEN c.age < 25 THEN 'Under 25'
            WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
            WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
            WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
            ELSE '55+' 
        END AS age_group,
        MAX(CASE WHEN f.default_flag THEN 1 ELSE 0 END) AS ever_default
    FROM gold.fact_credit f
    JOIN gold.dim_client c ON f.client_id = c.client_id
    GROUP BY c.client_id, c.age
)
SELECT 
    age_group,
    COUNT(*) AS total_clients,
    SUM(ever_default) AS total_default_clients,
    ROUND(100.0 * SUM(ever_default) / COUNT(*), 2) AS default_rate_pct
FROM client_default
GROUP BY age_group
ORDER BY age_group;


-- Limit kredit → apakah default lebih banyak pada limit kecil/besar.

WITH client_default AS (
    SELECT 
        c.client_id,
        c.limit_bal,
        CASE 
            WHEN c.limit_bal < 20000000 THEN '11 Juta - <20 Juta'
            WHEN c.limit_bal BETWEEN 20000000 AND 29999999 THEN '20 Juta - <30 Juta'
            WHEN c.limit_bal BETWEEN 30000000 AND 39999999 THEN '30 Juta - <40 Juta'
            WHEN c.limit_bal BETWEEN 40000000 AND 49999999 THEN '40 Juta - <50 Juta'
            WHEN c.limit_bal BETWEEN 50000000 AND 59999999 THEN '50 Juta - <60 Juta'
            WHEN c.limit_bal BETWEEN 60000000 AND 69999999 THEN '60 Juta - <70 Juta'
            WHEN c.limit_bal BETWEEN 70000000 AND 79999999 THEN '70 Juta - <80 Juta'
            WHEN c.limit_bal BETWEEN 80000000 AND 89999999 THEN '80 Juta - <90 Juta'
            ELSE '90 Juta - 100 Juta'
        END AS limit_group,
        MAX(CASE WHEN f.default_flag THEN 1 ELSE 0 END) AS ever_default
    FROM gold.fact_credit f
    JOIN gold.dim_client c ON f.client_id = c.client_id
    GROUP BY c.client_id, c.limit_bal
)
SELECT 
    limit_group,
    COUNT(*) AS total_clients,
    SUM(ever_default) AS total_default_clients,
    ROUND(100.0 * SUM(ever_default) / COUNT(*), 2) AS default_rate_pct
FROM client_default
GROUP BY limit_group
ORDER BY limit_group;
