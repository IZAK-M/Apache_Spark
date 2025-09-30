# Apache_Spark

# Nettoyage des données avec Apache Spark

Ce projet utilise Apache Spark pour nettoyer le fichier de ventes brutes.  
Le processus se déroule en deux étapes :

1. **Exploration & tests** : réalisés dans le notebook, afin de définir la logique de nettoyage.
2. **Nettoyage final** : implémenté dans `clean_data.py`, destiné à être exécuté en production.

## Pré-requis
- Créez un environnement conforme au fichier `requirements.txt`.
- Placez le fichier brut `ventes.csv` dans le dossier `data/`.

## Exécution
Lancez simplement `clean_data.py`.  
Le dossier `data/` contiendra ensuite :
- `ventes_clean.csv` : données nettoyées  
- `ventes_rejetees.csv` : données invalides
