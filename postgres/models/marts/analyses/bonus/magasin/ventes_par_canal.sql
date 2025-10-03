-- Quelle part de ventes vient des boutiques physiques vs de l’e-shop ?
SELECT
  m.magasin_type,
  ROUND(SUM(f.montant_total)::numeric, 2) AS chiffre_affaires,
  ROUND(100.0 * SUM(f.montant_total)::numeric / SUM(SUM(f.montant_total)::numeric) OVER (), 2) AS part_pourcentage
FROM  {{ ref('fact_ventes') }} AS f
LEFT JOIN {{ ref('dim_magasins') }} AS m
  ON f.magasin_id = m.magasin_id
GROUP BY m.magasin_type
ORDER BY part_pourcentage DESC