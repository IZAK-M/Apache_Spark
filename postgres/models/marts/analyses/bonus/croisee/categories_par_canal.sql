-- Quelles catégories sont les plus vendues sur le e-shop vs en boutique ?
SELECT
  m.magasin_type,
  p.produit_categorie,
  SUM(f.quantite) AS total_quantite
FROM {{ ref('fact_ventes') }} AS f
LEFT JOIN {{ ref('dim_magasins') }} AS m
  ON f.magasin_id = m.magasin_id
LEFT JOIN {{ ref('dim_produits') }} AS p
  ON f.produit_id = p.produit_id
GROUP BY m.magasin_type, p.produit_categorie
ORDER BY m.magasin_type, total_quantite DESC
