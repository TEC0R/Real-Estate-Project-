from airflow.providers.mysql.hooks.mysql import MySqlHook
import requests
import json
import logging
from mysql.connector import Error
from contextlib import closing
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy.types import String, Text, DateTime
from sqlalchemy import create_engine
from typing import List, Dict, Any
import concurrent.futures
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from functools import partial
from typing import List, Dict, Any, Tuple
import time
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
ID_CONN = "MYSQL_DATA"
TABLE_NAME = "bienici"
BATCH_SIZE = 24
MAX_RETRIES = 3
TIMEOUT = 30
MAX_WORKERS = 4  # Nombre de workers pour le threading
CHUNK_SIZE = 100  # Taille des chunks pour l'insertion SQL
MAX_RETRIES = 5
RETRY_WAIT_SECONDS = 2

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
    'Origin': 'https://www.bienici.com',
    'Referer': 'https://www.bienici.com/'
}

def get_mysql_connection() -> str:
    """Récupère l'URL de connexion MySQL depuis Airflow"""
    try:
        mysql_hook = MySqlHook(mysql_conn_id=ID_CONN)
        connection = mysql_hook.get_sqlalchemy_engine().url
        logger.info("✅ Connexion MySQL récupérée avec succès")
        return str(connection)
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération de la connexion MySQL: {e}")
        raise

class BienIciScraper:
    def __init__(self):
        self.session = self._create_session()
        self.engine = create_engine(get_mysql_connection(), pool_size=MAX_WORKERS, max_overflow=2)
        self.metrics = {
            'total_ads': 0,
            'failed_cities': [],
            'processing_time': 0
        }

    def _create_session(self) -> requests.Session:
        """Crée une session HTTP configurée"""
        session = requests.Session()
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=RETRY_WAIT_SECONDS,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(HEADERS)  # Utilisez HEADERS au lieu de headers
        return session

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def get_zone_ids(self, zip_code: str) -> List[str]:
        """Version améliorée avec retry automatique"""
        url = "https://res.bienici.com/suggest.json"
        params = {"q": zip_code, "type": "city,delegated-city,department,postalCode,region"}
        
        response = self.session.get(url, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        
        for item in response.json():
            if zip_code in item.get("postalCodes", []):
                return item.get("zoneIds", [])
        return []

    def process_ads_batch(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Version optimisée du traitement des annonces"""
        if not data.get("realEstateAds"):
            return pd.DataFrame()

        df = pd.json_normalize(data["realEstateAds"])
        if df.empty:
            return df

        df['modificationDate'] = pd.to_datetime(df['modificationDate'], utc=True)
        date_now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        before_yesterday = date_now - timedelta(days=1)
        
        mask = (before_yesterday < df['modificationDate']) & (df['modificationDate'] < date_now)
        df = df[mask].copy()
        
        if not df.empty:
            df['modificationDate'] = df['modificationDate'].dt.strftime('%Y-%m-%dT%H:%M:%S')
            df['data'] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
            df['timestamp'] = datetime.now()
            
        return df[['id', 'data', 'timestamp']]

    def extract_data(self, city_data: Tuple[str, str, str]) -> int:
        """Extraction optimisée des données pour une ville"""
        ville, zip_code, zone_ids = city_data
        logger.info(f"Traitement de {ville} ({zip_code})")
        
        total_ads = 0
        from_page = 0
        
        while True:
            try:
                url = "https://www.bienici.com/realEstateAds.json"
                filters = {
                    "size": BATCH_SIZE,
                    "from": from_page,
                    "filterType": "buy",
                    "propertyType": ["house", "flat"],
                    "newProperty": False,
                    "sortBy": "modificationDate",
                    "sortOrder": "desc",
                    "onTheMarket": [True],
                    "zoneIdsByTypes": {"zoneIds": zone_ids.split(',')}
                }

                params = {
                    "filters": json.dumps(filters),
                    "extensionType": "extendedIfNoResult"
                }

                response = self.session.get(url, params=params, timeout=TIMEOUT)
                response.raise_for_status()
                
                df = self.process_ads_batch(response.json())
                if df.empty:
                    break

                self._bulk_insert(df)
                
                batch_size = len(df)
                total_ads += batch_size
                from_page += BATCH_SIZE

                if batch_size < BATCH_SIZE:
                    break

            except Exception as e:
                logger.error(f"Erreur pour {ville}: {e}")
                break

        return total_ads

    def _bulk_insert(self, df: pd.DataFrame) -> None:
        """Insertion optimisée en base de données"""
        if df.empty:
            return

        dtype = {
            'id': String(length=100),
            'data': Text(),
            'timestamp': DateTime()
        }

        with self.engine.begin() as connection:
            df.to_sql(
                TABLE_NAME,
                con=connection,
                dtype=dtype,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=CHUNK_SIZE
            )

    def extract_france(self) -> Dict[str, Any]:
        """Version parallélisée de l'extraction"""
        start_time = time.time()
        
        with open('/opt/airflow/dags/scraping/cities.json', 'r') as f:
            cities = pd.DataFrame(json.load(f)['cities'])
        
        idf_cities = cities[cities['region_name'] == 'île-de-france'].reset_index(drop=True)
        
        # Préparation des données pour le traitement parallèle
        city_data = []
        for _, city in idf_cities.iterrows():
            try:
                zone_ids = self.get_zone_ids(city['zip_code'])
                if zone_ids:
                    city_data.append((
                        city['label'].title(),
                        city['zip_code'],
                        ','.join(zone_ids)
                    ))
            except Exception as e:
                logger.error(f"Erreur lors de la récupération des zone_ids pour {city['label']}: {e}")
                self.metrics['failed_cities'].append(city['label'])

        # Traitement parallèle
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(self.extract_data, city_data))

        self.metrics['total_ads'] = sum(results)
        self.metrics['processing_time'] = time.time() - start_time

        if self.metrics['total_ads'] == 0:
            raise ValueError("Aucune annonce récupérée")

        return self.metrics

def main():
    scraper = BienIciScraper()
    try:
        metrics = scraper.extract_france()
        logger.info(f"""
        🎯 Résultats du scraping:
        - Total annonces: {metrics['total_ads']}
        - Temps de traitement: {metrics['processing_time']:.2f} secondes
        - Villes en échec: {len(metrics['failed_cities'])}
        """)
    finally:
        scraper.engine.dispose()

if __name__ == "__main__":
    main()