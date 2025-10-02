SELECT
  TO_CHAR(date_vente, 'YYYY-MM') AS mois,
  SUM(montant_total) AS chiffre_affaires
FROM {{ ref('fact_ventes') }}
GROUP BY mois
ORDER BY chiffre_affaires DESC
