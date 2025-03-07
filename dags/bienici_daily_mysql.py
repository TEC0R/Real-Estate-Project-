from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from scraping.bienici import BienIciScraper
import logging
import sys

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Arguments par défaut du DAG avec plus de configurations
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 13),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),  # Timeout maximum
    "on_failure_callback": None,
    "provide_context": True
}

def run_scraper(**context):
    """Fonction principale pour exécuter le scraper avec gestion d'erreurs améliorée"""
    try:
        scraper = BienIciScraper()
        metrics = scraper.extract_france()
        
        # Log des résultats
        logger.info(f"""
        🎯 Résultats du scraping:
        - Total annonces: {metrics['total_ads']}
        - Temps de traitement: {metrics['processing_time']:.2f} secondes
        - Villes en échec: {len(metrics['failed_cities'])}
        """)

        # Vérification des résultats
        if metrics['total_ads'] == 0:
            raise AirflowException("❌ Aucune annonce récupérée")
        
        if len(metrics['failed_cities']) > 10:  # Seuil arbitraire
            logger.warning(f"⚠️ Nombre important de villes en échec: {len(metrics['failed_cities'])}")
        
        # Push des métriques dans XCOM pour utilisation ultérieure
        context['task_instance'].xcom_push('scraping_metrics', metrics)
        return metrics

    except Exception as e:
        logger.error(f"❌ Erreur critique lors du scraping: {str(e)}")
        raise AirflowException(f"Échec du scraping: {str(e)}")
    
    finally:
        if 'scraper' in locals():
            scraper.engine.dispose()

with DAG(
    "bienici_daily",
    default_args=default_args,
    description="ETL des annonces immobilières depuis BienIci",
    schedule_interval="0 22 * * *",
    catchup=False,
    tags=["immobilier", "bienici"],
    doc_md="""
    # DAG de scraping BienIci
    
    Ce DAG effectue le scraping quotidien des annonces immobilières de BienIci.
    
    ## Tâches
    - check_environment: Vérifie l'environnement d'exécution
    - extract_data: Récupère les données de BienIci
    
    ## Dépendances
    - Nécessite une connexion MySQL configurée avec l'ID 'MYSQL_DATA'
    """
) as dag:

    check_env = BashOperator(
        task_id="check_environment",
        bash_command="echo 'Environnement OK - $(date)'",
        dag=dag
    )

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=run_scraper,
        provide_context=True,
        dag=dag
    )

    # Définition du flux de tâches
    check_env >> extract