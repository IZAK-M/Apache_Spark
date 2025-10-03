-- Quels produits sont achetés par les jeunes clients (<30 ans) ?
SELECT
  p.produit_nom,
  COUNT(*) AS nb_ventes
FROM {{ ref('fact_ventes') }} AS f
LEFT JOIN {{ ref('dim_clients') }} AS c
  ON f.client_id = c.client_id
LEFT JOIN {{ ref('dim_produits') }} AS p
  ON f.produit_id = p.produit_id
WHERE c.client_age < 30
GROUP BY p.produit_nom
ORDER BY nb_ventes DESC
