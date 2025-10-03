SELECT
  m.magasin_region,
  SUM(f.montant_total) AS chiffre_affaires
FROM {{ ref('fact_ventes') }} AS f
LEFT JOIN {{ ref('dim_magasins') }} AS m
  ON f.magasin_id = m.magasin_id
GROUP BY m.magasin_region
ORDER BY chiffre_affaires DESC
