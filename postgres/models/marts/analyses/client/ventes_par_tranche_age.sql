SELECT
  FLOOR(c.client_age / 10) * 10 AS tranche_age,
  COUNT(*) AS nb_ventes,
  SUM(f.montant_total) AS chiffre_affaires
FROM {{ ref('fact_ventes') }} AS f
LEFT JOIN {{ ref('dim_clients') }} AS c
  ON f.client_id = c.client_id
GROUP BY tranche_age
ORDER BY tranche_age
