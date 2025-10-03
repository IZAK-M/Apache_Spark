SELECT
  f.produit_id,
  p.produit_nom,
  SUM(f.quantite) AS total_quantite
FROM {{ ref('fact_ventes') }} AS f
LEFT JOIN {{ ref('dim_produits') }} AS p
  ON f.produit_id = p.produit_id
GROUP BY f.produit_id, p.produit_nom
ORDER BY total_quantite DESC
