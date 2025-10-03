-- Existe-t-il une saisonnalité (ex: pics sur certains mois/produits) ?
SELECT
  TO_CHAR(f.date_vente, 'YYYY-MM') AS mois,
  p.produit_nom,
  SUM(f.montant_total) AS chiffre_affaires
FROM {{ ref('fact_ventes') }} AS f
LEFT JOIN {{ ref('dim_produits') }} AS p
  ON f.produit_id = p.produit_id
GROUP BY mois, p.produit_nom
ORDER BY p.produit_nom DESC
