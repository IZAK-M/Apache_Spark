SELECT
  f.produit_id,
  p.produit_nom,
  SUM(f.montant_total) AS chiffre_affaires
FROM {{ ref('fact_ventes') }} AS f
LEFT JOIN {{ ref('dim_produits') }} AS p
  ON f.produit_id = p.produit_id
GROUP BY f.produit_id, p.produit_nom
ORDER BY chiffre_affaires DESC

