{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['client_nom','client_age']) }} AS client_id,
    client_nom,
    client_age,
    client_ville
FROM raw_cleaned.stg_ventes