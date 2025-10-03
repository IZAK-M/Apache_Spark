{{ config(materialized='table') }}

SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['magasin_nom', 'magasin_region']) }} AS magasin_id,
	magasin_nom,
	magasin_type,
	magasin_region
FROM raw_cleaned.stg_ventes