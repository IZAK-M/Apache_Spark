{{ config(materialized='table')}}

SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['produit_nom']) }} AS produit_id,
	produit_nom,
	produit_categorie,
	produit_marque
FROM raw_cleaned.stg_ventes