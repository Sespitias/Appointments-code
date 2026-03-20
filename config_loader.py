import os
import yaml
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv


class Config:
    _instance: Optional['Config'] = None

    def __init__(self, config_path: str = None):
        self._config = {}
        self._load_env()
        if config_path:
            self.load_config(config_path)

    @classmethod
    def get_instance(cls, config_path: str = None) -> 'Config':
        if cls._instance is None:
            cls._instance = cls(config_path)
        return cls._instance

    def _load_env(self):
        env_path = Path(__file__).parent / '.env'
        if env_path.exists():
            load_dotenv(env_path)

        self._config['tebra'] = {
            'wsdl': os.getenv('TEBRA_WSDL', 'https://webservice.kareo.com/services/soap/2.1/KareoServices.svc?singleWsdl'),
            'user': os.getenv('TEBRA_USER'),
            'password': os.getenv('TEBRA_PASSWORD'),
            'customer_key': os.getenv('TEBRA_CUSTOMER_KEY')
        }

        self._config['bigquery'] = {
            'project_id': os.getenv('GCP_PROJECT_ID'),
            'dataset_id': os.getenv('BQ_DATASET_ID'),
            'source_table': os.getenv('BQ_SOURCE_TABLE', 'appointment_update_copy'),
            'target_table': os.getenv('BQ_TARGET_TABLE', 'appointment_prod_copy')
        }

        self._config['google_sheets'] = {
            'sheet_key': os.getenv('SHEET_KEY'),
            'patient_sheet_key': os.getenv('PATIENT_SHEET_KEY'),
            'insurance_sheet_key': os.getenv('INSURANCE_SHEET_KEY')
        }

        self._config['pipeline'] = {
            'months_back': int(os.getenv('MONTHS_BACK', 1)),
            'months_forward': int(os.getenv('MONTHS_FORWARD', 2)),
            'fetch_interval_days': int(os.getenv('FETCH_INTERVAL_DAYS', 8)),
            'parallel_workers': int(os.getenv('PARALLEL_WORKERS', 10)),
            'async_upload': os.getenv('ASYNC_UPLOAD', 'true').lower() == 'true',
            'chunk_size': int(os.getenv('CHUNK_SIZE', 500))
        }

        self._config['execution'] = {
            'type': int(os.getenv('EXECUTION_TYPE', 2)),
            'days_back_filter': int(os.getenv('DAYS_BACK_FILTER', 10))
        }

    def load_config(self, config_path: str):
        path = Path(config_path)
        if path.exists():
            with open(path, 'r') as f:
                yaml_config = yaml.safe_load(f)
                if yaml_config:
                    self._merge_config(yaml_config)

    def _merge_config(self, yaml_config: dict):
        for key, value in yaml_config.items():
            if key in self._config and isinstance(value, dict):
                self._config[key].update(value)
            else:
                self._config[key] = value

    def get(self, key: str, default=None):
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default

    @property
    def tebra(self) -> dict:
        return self._config.get('tebra', {})

    @property
    def bigquery(self) -> dict:
        return self._config.get('bigquery', {})

    @property
    def google_sheets(self) -> dict:
        return self._config.get('google_sheets', {})

    @property
    def pipeline(self) -> dict:
        return self._config.get('pipeline', {})

    @property
    def execution(self) -> dict:
        return self._config.get('execution', {})

    def validate(self) -> list:
        errors = []
        required = {
            'tebra': ['user', 'password', 'customer_key'],
            'bigquery': ['project_id', 'dataset_id'],
            'google_sheets': ['sheet_key']
        }

        for section, fields in required.items():
            for field in fields:
                if not self._config.get(section, {}).get(field):
                    errors.append(f"Missing config: {section}.{field}")

        service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        oauth_fields = ['CLIENT_ID', 'CLIENT_SECRET', 'REFRESH_TOKEN']
        oauth_values = {field: os.getenv(field) for field in oauth_fields}

        if service_account_path:
            if not Path(service_account_path).exists():
                errors.append(
                    f"GOOGLE_APPLICATION_CREDENTIALS points to a missing file: {service_account_path}"
                )
        else:
            missing_oauth = [field for field, value in oauth_values.items() if not value]
            if missing_oauth:
                errors.append(
                    "Missing Google auth configuration. Set GOOGLE_APPLICATION_CREDENTIALS "
                    f"or provide: {', '.join(missing_oauth)}"
                )

        return errors


def get_config(config_path: str = None) -> Config:
    return Config.get_instance(config_path)
