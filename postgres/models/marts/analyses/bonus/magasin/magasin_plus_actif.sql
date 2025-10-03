# Quel magasin physique génère le plus de transactions ?
SELECT
  m.magasin_nom,
  COUNT(DISTINCT f.id_transaction) AS nb_transactions
FROM {{ ref('fact_ventes') }} AS f
LEFT JOIN {{ ref('dim_magasins') }} AS m
  ON f.magasin_id = m.magasin_id
WHERE m.magasin_type = 'physique'
GROUP BY m.magasin_nom
ORDER BY nb_transactions DESC