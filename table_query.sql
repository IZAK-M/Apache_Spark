-- Source 
SELECT *
FROM raw_cleaned.stg_ventes;

-- table dim_client
SELECT 
	DISTINCT client_nom,
    client_age,
    client_ville
FROM raw_cleaned.stg_ventes;

-- table dim_magasin
SELECT 
	DISTINCT magasin_nom,
	magasin_type,
	magasin_region
FROM raw_cleaned.stg_ventes
GROUP BY 1,2,3;

-- table dim_produit
SELECT
	DISTINCT produit_nom,
	produit_categorie,
	produit_marque
FROM raw_cleaned.stg_ventes
GROUP BY 1,2,3;

-- table dim_temps
SELECT 
	DISTINCT date_vente,
	EXTRACT(YEAR FROM date_vente) AS annee,
	DATE_PART('month', date_vente) AS mois,
	DATE_PART('day', date_vente) AS jour,
	TO_CHAR(date_vente, 'day') AS jour_semaine,
	DATE_PART('quarter', date_vente) AS Trimestre
FROM raw_cleaned.stg_ventes
ORDER BY date_vente;

-- table fact_ventes (Ne pas exécuter.)
SELECT
	id_transaction,
	client_id,
	produit_id,
	prix_catalogue,
	magasin_id,
	date_vente,
	quantite,
	montant_total
FROM raw_cleaned.stg_ventes;



