import os
import sys
import time
import signal
import logging
import json
from datetime import datetime
from pathlib import Path
import requests
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, DuplicateKeyError
from dotenv import load_dotenv
import sentry_sdk

load_dotenv()

START_PAGE = int(os.getenv('START_PAGE'))
END_PAGE = int(os.getenv('END_PAGE'))

DEFAULT_BATCH_SIZE = 100
DEFAULT_REQUEST_DELAY = 0.01
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3
HEARTBEAT_INTERVAL = 300
STATUS_UPDATE_INTERVAL = 60

# ─── add just below load_dotenv() ──────────────────────────────
PERSIST_DIR = os.getenv("PERSIST_DIR", "/data")  # default path inside container
Path(PERSIST_DIR).mkdir(parents=True, exist_ok=True)

STATE_FILE = os.path.join(PERSIST_DIR, f"export_state_{START_PAGE}.json")
LOG_FILE   = os.path.join(PERSIST_DIR, f"export_{START_PAGE}.log")
# ───────────────────────────────────────────────────────────────


# STATE_FILE = f"export_state_{START_PAGE}.json"
# LOG_FILE = f"export_{START_PAGE}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

if os.getenv('SENTRY_DSN'):
    sentry_sdk.init(
        dsn=os.getenv('SENTRY_DSN'),
        traces_sample_rate=1.0,
        environment=os.getenv('ENVIRONMENT', 'production')
    )


class Config:
    def __init__(self):
        self.API_URL = os.getenv('API_URL')
        self.MONGO_URI = os.getenv('MONGO_URI')
        self.DB_NAME = os.getenv('DB_NAME', 'sharpbuy')
        self.COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'New_Import')

        self.HEADERS = {
            "Content-Type": "application/json",
        }
        if os.getenv('API_TOKEN'):
            self.HEADERS["Authorization"] = f"Bearer {os.getenv('API_TOKEN')}"

        self.PARAMS = {
            "form_date": "2024-04-01",
            "to_date": "2025-03-31",
            "type": 1,
            "page": 1,
            "size": int(os.getenv('BATCH_SIZE', DEFAULT_BATCH_SIZE))
        }

        self.validate()

    def validate(self):
        if not self.API_URL:
            raise ValueError("API_URL must be set")
        if not self.MONGO_URI:
            raise ValueError("MONGO_URI must be set")


class ImportState:
    def __init__(self):
        self.start_time = datetime.now()
        self.last_status_update = time.time()
        self.last_heartbeat = time.time()
        self.shutdown_requested = False
        self.current_page = START_PAGE
        self.stats = {
            'pages': 0,
            'processed': 0,
            'inserted': 0,
            'errors': 0,
            'consecutive_errors': 0,
            'last_successful_page': 0
        }
        self.load_state()

    def load_state(self):
        try:
            if Path(STATE_FILE).exists():
                with open(STATE_FILE, 'r') as f:
                    state_data = json.load(f)
                    self.current_page = max(state_data.get('current_page', START_PAGE), START_PAGE)
                    self.stats = state_data.get('stats', self.stats)
                    logger.info(f"Resuming from page {self.current_page}")
        except Exception as e:
            logger.warning(f"Failed to load state: {e}")

    def save_state(self):
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump({
                    'current_page': self.current_page,
                    'stats': self.stats,
                    'timestamp': datetime.now().isoformat()
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving state: {e}")

    def update_heartbeat(self):
        self.last_heartbeat = time.time()

    def should_update_status(self):
        return time.time() - self.last_status_update > STATUS_UPDATE_INTERVAL

    def update_status_time(self):
        self.last_status_update = time.time()

    def record_success(self, page_num, records_processed, inserted):
        self.current_page = page_num + 1
        self.stats['pages'] += 1
        self.stats['processed'] += records_processed
        self.stats['inserted'] += inserted
        self.stats['consecutive_errors'] = 0
        self.stats['last_successful_page'] = page_num
        self.save_state()

    def record_error(self):
        self.stats['errors'] += 1
        self.stats['consecutive_errors'] += 1
        self.save_state()

    def get_processing_rate(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return self.stats['processed'] / max(elapsed, 1)

    def get_elapsed_time(self):
        return datetime.now() - self.start_time

    def cleanup(self):
        try:
            Path(STATE_FILE).unlink(missing_ok=True)
            logger.info("State file cleaned up")
        except Exception as e:
            logger.warning(f"Failed to clean state: {e}")


def setup_signal_handlers(state):
    def handler(sig, frame):
        logger.info("Shutdown requested")
        state.shutdown_requested = True
        state.save_state()
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def get_api_page(page_num, config):
    for attempt in range(DEFAULT_MAX_RETRIES + 1):
        try:
            response = requests.get(
                config.API_URL,
                headers=config.HEADERS,
                params={**config.PARAMS, "page": page_num},
                timeout=DEFAULT_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                for key in data:
                    if isinstance(data[key], list):
                        return data[key]
                return []
            return []
        except Exception as e:
            logger.error(f"Fetch failed for page {page_num}: {e}")
            time.sleep(2 ** attempt)
    return None


def process_records(records, page_num, collection):
    if not records:
        return 0, 0

    valid_records = []
    for record in records:
        try:
            if not record.get('id'):
                continue
            record['id'] = str(record['id'])  # Ensure id is a string
            valid_records.append(record)
        except Exception as e:
            logger.warning(f"Record processing error (page {page_num}): {e}")

    if valid_records:
        try:
            result = collection.insert_many(valid_records, ordered=False)
            return len(valid_records), len(result.inserted_ids)
        except BulkWriteError as bwe:
            logger.warning(f"Duplicate insert skipped (page {page_num})")
            return len(valid_records), len(valid_records) - len(bwe.details.get('writeErrors', []))
        except DuplicateKeyError as dke:
            logger.warning(f"DuplicateKeyError on page {page_num}: {dke}")
            return len(valid_records), 0
        except Exception as e:
            logger.error(f"Insert failed on page {page_num}: {e}")
            return len(valid_records), 0
    return 0, 0


def send_heartbeat(state):
    if time.time() - state.last_heartbeat > HEARTBEAT_INTERVAL:
        state.update_heartbeat()


def log_status(state):
    elapsed = state.get_elapsed_time()
    rate = state.get_processing_rate()
    logger.info(
        f"Page: {state.current_page} | "
        f"Processed: {state.stats['processed']:,} | "
        f"Inserted: {state.stats['inserted']:,} | "
        f"Errors: {state.stats['errors']} | "
        f"Rate: {rate:.1f}/s | "
        f"Elapsed: {str(elapsed).split('.')[0]}"
    )
    state.update_status_time()


def verify_connectivity(config):
    logger.info("Verifying connectivity...")
    try:
        test = requests.get(
            config.API_URL,
            headers=config.HEADERS,
            params={**config.PARAMS, "page": 1, "size": 1},
            timeout=DEFAULT_TIMEOUT
        )
        test.raise_for_status()
        logger.info("API connection OK")
    except Exception as e:
        logger.error(f"API connection failed: {e}")
        raise

    try:
        client = MongoClient(config.MONGO_URI)
        client.server_info()
        logger.info("MongoDB connection OK")
        client.close()
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise


def run_import():
    try:
        config = Config()
        verify_connectivity(config)
    except Exception as e:
        logger.error("Startup validation failed. Exiting.")
        sys.exit(1)

    state = ImportState()
    setup_signal_handlers(state)

    logger.info("Starting import process")
    logger.info(f"Date Range: {config.PARAMS['form_date']} to {config.PARAMS['to_date']}")
    logger.info(f"MongoDB Collection: {config.DB_NAME}.{config.COLLECTION_NAME}")
    logger.info(f"Page Range: {START_PAGE} to {END_PAGE}")

    try:
        client = MongoClient(config.MONGO_URI)
        db = client[config.DB_NAME]
        collection = db[config.COLLECTION_NAME]
        collection.create_index("id", unique=True)

        page_num = max(state.current_page, START_PAGE)

        while page_num <= END_PAGE and not state.shutdown_requested:
            if state.should_update_status():
                log_status(state)

            send_heartbeat(state)

            records = get_api_page(page_num, config)
            if records is None:
                state.record_error()
                if state.stats['consecutive_errors'] >= 3:
                    logger.error("Too many errors. Stopping.")
                    break
                page_num += 1
                continue

            if not records:
                logger.info(f"No records on page {page_num}. Skipping.")
                page_num += 1
                continue

            processed, inserted = process_records(records, page_num, collection)
            state.record_success(page_num, processed, inserted)

            page_num += 1
            time.sleep(DEFAULT_REQUEST_DELAY)

        state.cleanup()

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        if os.getenv('SENTRY_DSN'):
            sentry_sdk.capture_exception(e)
    finally:
        client.close()
        logger.info("\n=== IMPORT SUMMARY ===")
        logger.info(f"Pages processed: {state.stats['pages']}")
        logger.info(f"Records received: {state.stats['processed']:,}")
        logger.info(f"Records inserted: {state.stats['inserted']:,}")
        logger.info(f"Errors: {state.stats['errors']}")
        logger.info(f"Elapsed time: {state.get_elapsed_time()}")
        logger.info(f"Rate: {state.get_processing_rate():.2f} records/sec")

        if state.shutdown_requested:
            logger.info("Import was interrupted.")
            sys.exit(130)
        elif state.stats['consecutive_errors'] >= 3:
            sys.exit(1)


if __name__ == "__main__":
    run_import()
