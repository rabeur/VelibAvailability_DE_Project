#!/bin/bash
###############################################################################
# Script de déploiement - Phase 3 : Silver Layer
# Vélib Data Engineering Project
###############################################################################

set -e  # Arrêter en cas d'erreur

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fonctions utilitaires
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Banner
echo "============================================================================"
echo "🚀 DÉPLOIEMENT PHASE 3 - SILVER LAYER"
echo "============================================================================"
echo ""

# Vérifier que nous sommes dans le bon répertoire
if [ ! -f "docker-compose.yml" ]; then
    log_error "docker-compose.yml non trouvé. Exécutez ce script depuis la racine du projet."
    exit 1
fi

log_info "Répertoire de travail: $(pwd)"
echo ""

###############################################################################
# ÉTAPE 1: Vérifier les prérequis
###############################################################################

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 ÉTAPE 1/6 : Vérification des prérequis"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Vérifier Docker
if ! command -v docker &> /dev/null; then
    log_error "Docker n'est pas installé"
    exit 1
fi
log_success "Docker installé"

# Vérifier Docker Compose
if ! command -v docker-compose &> /dev/null; then
    log_error "Docker Compose n'est pas installé"
    exit 1
fi
log_success "Docker Compose installé"

# Vérifier que les conteneurs sont en cours d'exécution
CONTAINERS=("velib_postgres" "velib_spark" "velib_airflow_scheduler")
for container in "${CONTAINERS[@]}"; do
    if ! docker ps | grep -q "$container"; then
        log_error "Conteneur $container non démarré"
        log_info "Lancez 'docker-compose up -d' d'abord"
        exit 1
    fi
    log_success "Conteneur $container opérationnel"
done

echo ""

###############################################################################
# ÉTAPE 2: Initialiser le schéma PostgreSQL Silver
###############################################################################

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🗄️  ÉTAPE 2/6 : Initialisation du schéma PostgreSQL Silver"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ ! -f "sql/02_init_silver_schema.sql" ]; then
    log_error "Fichier sql/02_init_silver_schema.sql non trouvé"
    exit 1
fi

log_info "Exécution du script SQL..."
docker exec -i velib_postgres psql -U velib -d velib_dw < sql/02_init_silver_schema.sql

# Vérifier que les tables ont été créées
TABLE_COUNT=$(docker exec -i velib_postgres psql -U velib -d velib_dw -t -c "
    SELECT COUNT(*) FROM information_schema.tables
    WHERE table_schema = 'silver' AND table_type = 'BASE TABLE';
" | tr -d ' ')

if [ "$TABLE_COUNT" -ge 2 ]; then
    log_success "Schéma Silver créé ($TABLE_COUNT tables)"
else
    log_error "Échec de la création du schéma Silver"
    exit 1
fi

echo ""

###############################################################################
# ÉTAPE 3: Installer le driver JDBC PostgreSQL dans Spark
###############################################################################

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔌 ÉTAPE 3/6 : Installation du driver JDBC PostgreSQL"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Vérifier si le driver existe déjà
if docker exec velib_spark test -f /opt/spark/jars/postgresql-42.7.1.jar; then
    log_success "Driver JDBC déjà installé"
else
    log_info "Téléchargement du driver JDBC..."
    docker exec velib_spark bash -c "
        mkdir -p /opt/spark/jars &&
        cd /opt/spark/jars &&
        wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
    "

    if docker exec velib_spark test -f /opt/spark/jars/postgresql-42.7.1.jar; then
        log_success "Driver JDBC installé"
    else
        log_error "Échec de l'installation du driver JDBC"
        exit 1
    fi
fi

echo ""

###############################################################################
# ÉTAPE 4: Déployer le script PySpark
###############################################################################

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "⚙️  ÉTAPE 4/6 : Déploiement du script PySpark"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Créer le dossier spark_jobs s'il n'existe pas
mkdir -p spark_jobs

# Copier le script (à ajuster selon où vous avez téléchargé les fichiers)
if [ -f "bronze_to_silver.py" ]; then
    cp bronze_to_silver.py spark_jobs/
    log_info "Script copié dans spark_jobs/"
elif [ -f "spark_jobs/bronze_to_silver.py" ]; then
    log_info "Script déjà présent dans spark_jobs/"
else
    log_error "Fichier bronze_to_silver.py non trouvé"
    log_info "Placez-le dans le répertoire courant ou dans spark_jobs/"
    exit 1
fi

# Vérifier que Spark peut accéder au script
if docker exec velib_spark test -f /opt/spark_jobs/bronze_to_silver.py; then
    log_success "Script accessible par Spark"
else
    log_error "Script non accessible par Spark"
    log_info "Vérifiez le montage du volume dans docker-compose.yml"
    exit 1
fi

echo ""

###############################################################################
# ÉTAPE 5: Déployer le DAG Airflow
###############################################################################

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📅 ÉTAPE 5/6 : Déploiement du DAG Airflow"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Créer le dossier airflow/dags s'il n'existe pas
mkdir -p airflow/dags

# Copier le DAG
if [ -f "velib_silver_daily_transformation_dag.py" ]; then
    cp velib_silver_daily_transformation_dag.py airflow/dags/
    log_info "DAG copié dans airflow/dags/"
elif [ -f "airflow/dags/velib_silver_daily_transformation_dag.py" ]; then
    log_info "DAG déjà présent dans airflow/dags/"
else
    log_error "Fichier velib_silver_daily_transformation_dag.py non trouvé"
    exit 1
fi

# Attendre que Airflow détecte le nouveau DAG
log_info "Attente de la détection du DAG par Airflow (30s)..."
sleep 30

# Vérifier que le DAG est chargé
if docker exec velib_airflow_scheduler airflow dags list | grep -q "velib_silver_daily_transformation"; then
    log_success "DAG détecté par Airflow"
else
    log_warning "DAG non détecté (peut nécessiter plus de temps)"
    log_info "Vérifiez manuellement avec: docker exec velib_airflow_scheduler airflow dags list"
fi

echo ""

###############################################################################
# ÉTAPE 6: Tests de validation
###############################################################################

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🧪 ÉTAPE 6/6 : Tests de validation"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Test 1: Vérifier les tables Silver
log_info "Test 1: Vérification des tables Silver..."
docker exec -i velib_postgres psql -U velib -d velib_dw -t -c "
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'silver'
    ORDER BY table_name;
" | while read -r table; do
    table=$(echo "$table" | xargs)  # Trim whitespace
    if [ ! -z "$table" ]; then
        log_success "  Table: $table"
    fi
done

# Test 2: Vérifier les vues
log_info "Test 2: Vérification des vues..."
docker exec -i velib_postgres psql -U velib -d velib_dw -t -c "
    SELECT table_name FROM information_schema.views
    WHERE table_schema = 'silver'
    ORDER BY table_name;
" | while read -r view; do
    view=$(echo "$view" | xargs)
    if [ ! -z "$view" ]; then
        log_success "  Vue: $view"
    fi
done

echo ""

###############################################################################
# RÉSUMÉ ET PROCHAINES ÉTAPES
###############################################################################

echo "============================================================================"
echo "✨ DÉPLOIEMENT TERMINÉ AVEC SUCCÈS"
echo "============================================================================"
echo ""
echo "📊 Résumé du déploiement:"
echo "  ✅ Schéma PostgreSQL Silver initialisé"
echo "  ✅ Driver JDBC installé dans Spark"
echo "  ✅ Script PySpark déployé"
echo "  ✅ DAG Airflow déployé"
echo ""
echo "🧪 Pour tester le pipeline manuellement:"
echo ""
echo "  # Test Spark (date d'aujourd'hui)"
echo "  DATE_TODAY=\$(date +%Y-%m-%d)"
echo "  docker exec velib_spark /opt/spark/bin/spark-submit \\"
echo "    --master local[*] \\"
echo "    --driver-memory 2g \\"
echo "    --executor-memory 2g \\"
echo "    /opt/spark_jobs/bronze_to_silver.py \\"
echo "    /opt/data_lake/bronze/velib \\"
echo "    \$DATE_TODAY"
echo ""
echo "  # Déclencher le DAG Airflow"
echo "  docker exec velib_airflow_scheduler airflow dags trigger velib_silver_daily_transformation"
echo ""
echo "📈 Accès aux interfaces:"
echo "  - Airflow:   http://localhost:8081 (admin/admin)"
echo "  - Spark UI:  http://localhost:8080"
echo ""
echo "📚 Documentation complète:"
echo "  Consultez PHASE3_DEPLOYMENT_GUIDE.md pour plus de détails"
echo ""
echo "============================================================================"