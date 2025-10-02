{{ config(materialized='table') }}

SELECT 
	DISTINCT date_vente,
	EXTRACT(YEAR FROM date_vente) AS annee,
	DATE_PART('month', date_vente) AS mois,
	DATE_PART('day', date_vente) AS jour,
	TO_CHAR(date_vente, 'day') AS jour_semaine,
	DATE_PART('quarter', date_vente) AS Trimestre
FROM raw_cleaned.stg_ventes
ORDER BY date_vente