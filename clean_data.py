import os
import shutil
from colorama import Fore, Style

# Ajuste JAVA_HOME pour macOS (M1 / Homebrew OpenJDK 17)
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, abs, to_date, current_date, lit,
    lower, trim, regexp_replace, when
)

# Chemin relatif au projet (le script s'attend à être lancé depuis la racine du repo)
BASE_DIR = os.path.abspath(".")
CSV_PATH = os.path.join(BASE_DIR, "data", "ventes.csv")

# Initialiser Spark 
my_spark = SparkSession.builder.appName("NettoyerDonneesIncorrectes").getOrCreate()
my_spark.sparkContext.setLogLevel("ERROR")
my_spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Chargement du CSV d'origine
df = my_spark.read.csv(CSV_PATH, header=True, inferSchema=True)

print(f"Nombre de lignes avant nettoyage : {df.count()}")

# Normalisation des noms de colonnes (supprime espaces début/fin)
df = df.select([col(c).alias(c.strip()) for c in df.columns])

# Identification des colonnes de type string
string_cols = [col_name for col_name, dtype in df.dtypes if dtype == "string"]
print("Colonnes de type string :", string_cols)

# Nettoyage des chaînes : trim + lower
for col_name in string_cols:
    df = df.withColumn(col_name, lower(trim(col(col_name))))

# Correction des âges négatifs et des prix négatifs
if "client_age" in df.columns:
    df = df.withColumn("client_age", abs(col("client_age")))

if "prix_catalogue" in df.columns:
    df = df.withColumn("prix_catalogue", abs(col("prix_catalogue")))

# Robustifier quantite : normaliser, remplacer virgule décimale, cast en double
df = df.withColumn(
    "quantite",
    when((trim(col("quantite")) == "") | col("quantite").isNull(), None)
    .otherwise(
        regexp_replace(trim(col("quantite")), ",", ".")
    )
)

# S'assurer que prix_catalogue soit en double
df = df.withColumn("prix_catalogue", col("prix_catalogue").cast("double"))

# Calcul du montant_total 
df = df.withColumn("montant_total", col("prix_catalogue") * col("quantite"))

# Conversion de la colonne date (gestion si colonne nommée date ou date_vente)
if "date" in df.columns and "date_vente" not in df.columns:
    df = df.withColumnRenamed("date", "date_vente")

if "date_vente" in df.columns:
    df = df.withColumn("date_vente", to_date(col("date_vente"), "yyyy-MM-dd"))

# Plage de dates
date_min = to_date(lit("2023-01-01"))
date_max = current_date()

# Colonnes critiques à vérifier
cols_to_check = [
    "client_nom", "client_ville", "produit_nom",
    "produit_categorie", "produit_marque",
    "magasin_nom", "magasin_type", "magasin_region"
]
cols_to_check = [c for c in cols_to_check if c in df.columns]

def is_valid_string(column):
    return (
        ~col(column).rlike("^\\d+$") &
        ~col(column).isin("", "null", "n/a", "inconnu") &
        col(column).isNotNull()
    )

# Construction de la condition globale
condition = lit(True)
for col_name in cols_to_check:
    condition = condition & is_valid_string(col_name)

# Application des filres :
if "date_vente" in df.columns:
    df_cleaned = df.filter(
        (col("client_age")< 100) &
        (col("date_vente") >= date_min) &
        (col("date_vente") <= date_max) &
        condition &
        col("quantite").isNotNull() &
        col("prix_catalogue").isNotNull()
    ).dropna(how="any")
else:
    print("ATTENTION: colonne date_vente introuvable. Filtre de date ignoré.")
    df_cleaned = df.filter(
        condition &
        col("quantite").isNotNull() &
        col("prix_catalogue").isNotNull()
    ).dropna(how="any")



print(f"Nombre de lignes après nettoyage : {df_cleaned.count()}")
print("\nÉchantillon des données nettoyées :")
df_cleaned.show(10, truncate=False)

# Lignes rejetées
lignes_rejetees = df.subtract(df_cleaned)

# === Fonction utilitaire pour écrire un seul CSV ===
def save_single_csv(df, output_path):
    """Écrit un DataFrame Spark en un seul fichier CSV (output_path inclut le nom final)."""
    tmp_dir = output_path + "_tmp"

    # Supprimer anciens fichiers si existent
    if os.path.exists(output_path):
        os.remove(output_path)
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    # Écriture temporaire en plusieurs part-*
    df.coalesce(1).write.csv(tmp_dir, header=True, mode="overwrite")

    # Récupérer le part-*.csv généré
    part_files = [f for f in os.listdir(tmp_dir) if f.startswith("part-") and f.endswith(".csv")]
    if not part_files:
        raise RuntimeError(f"Aucun fichier part-*.csv trouvé dans {tmp_dir}")
    part_file = part_files[0]
    part_path = os.path.join(tmp_dir, part_file)

    # Déplacer/renommer vers output_path
    shutil.move(part_path, output_path)

    # Nettoyer le dossier temporaire
    shutil.rmtree(tmp_dir)

# Chemins finaux 
clean_csv = os.path.join(BASE_DIR, "data", "ventes_clean.csv")
rejetees_csv = os.path.join(BASE_DIR, "data", "ventes_rejetees.csv")

# Afficher le nombre de lignes nettoyées en vert
print(Fore.GREEN + f"🧼 {df_cleaned.count()} lignes nettoyées prêtes à être enregistrées." + Style.RESET_ALL)
save_single_csv(df_cleaned, clean_csv)
print(f"✅ Données nettoyées enregistrées dans {clean_csv}")

# Afficher le nombre de lignes rejetées en rouge
print(Fore.RED + f"🙅‍♂️ {lignes_rejetees.count()} lignes rejetées prêtes à être enregistrées." + Style.RESET_ALL)
save_single_csv(lignes_rejetees, rejetees_csv)
print(f"⚠️ Lignes rejetées enregistrées dans {rejetees_csv}")

my_spark.stop()
