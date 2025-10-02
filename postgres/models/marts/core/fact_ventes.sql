{{ config(materialized='table') }}

SELECT
	id_transaction,
	{{ dbt_utils.generate_surrogate_key(['client_nom', 'client_age']) }} AS client_id,
	{{ dbt_utils.generate_surrogate_key(['produit_nom']) }} AS produit_id,
	prix_catalogue,
	{{ dbt_utils.generate_surrogate_key(['magasin_nom', 'magasin_region']) }} AS magasin_id,
	date_vente,
	quantite,
	montant_total
FROM raw_cleaned.stg_ventes