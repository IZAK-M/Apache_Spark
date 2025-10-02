SELECT
  EXTRACT(YEAR FROM date_vente) AS annee,
  EXTRACT(MONTH FROM date_vente) AS mois,
  SUM(montant_total) AS chiffre_affaires
FROM {{ ref('fact_ventes') }}
GROUP BY annee, mois
ORDER BY annee, mois
