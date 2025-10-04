-- +-------------------------------------+
-- |Clés primaires dans les dimensions:  |
-- +-------------------------------------+

-- Clé primaire pour dim_clients
ALTER TABLE marts.dim_clients
ADD CONSTRAINT pk_client PRIMARY KEY (client_id);

-- Clé primaire pour dim_produits
ALTER TABLE marts.dim_produits
ADD CONSTRAINT pk_produit PRIMARY KEY (produit_id);

-- Clé primaire pour dim_magasins
ALTER TABLE marts.dim_magasins
ADD CONSTRAINT pk_magasin PRIMARY KEY (magasin_id);

-- Clé primaire pour dim_temps
ALTER TABLE marts.dim_temps
ADD CONSTRAINT pk_temps PRIMARY KEY (date_vente); 


-- +---------------------------------------+
-- |Clés étrangères dans la table de faits:|
-- +---------------------------------------+

-- FK vers dim_clients
ALTER TABLE marts.fact_ventes
ADD CONSTRAINT fk_client
FOREIGN KEY (client_id)
REFERENCES marts.dim_clients(client_id);

-- FK vers dim_produits
ALTER TABLE marts.fact_ventes
ADD CONSTRAINT fk_produit
FOREIGN KEY (produit_id)
REFERENCES marts.dim_produits(produit_id);

-- FK vers dim_magasins
ALTER TABLE marts.fact_ventes
ADD CONSTRAINT fk_magasin
FOREIGN KEY (magasin_id)
REFERENCES marts.dim_magasins(magasin_id);

-- FK vers dim_temps
ALTER TABLE marts.fact_ventes
ADD CONSTRAINT fk_temps
FOREIGN KEY (date_vente)
REFERENCES marts.dim_temps(date_vente); 


