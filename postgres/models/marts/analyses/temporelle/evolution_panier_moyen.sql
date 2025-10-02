SELECT
  TO_CHAR(date_vente, 'YYYY-MM') AS mois,
  COUNT(DISTINCT id_transaction) AS nb_transactions,
  SUM(montant_total) AS total_ventes,
  ROUND(SUM(montant_total)::numeric / COUNT(DISTINCT id_transaction), 2) AS panier_moyen
FROM {{ ref('fact_ventes') }}
GROUP BY mois
ORDER BY mois
