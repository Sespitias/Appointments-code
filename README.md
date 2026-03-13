# Appointment Pipeline - Cloud Run Deployment

Pipeline automatizado para extraer citas de Tebra, procesarlas y sincronizarlas con BigQuery y Google Sheets.

## Estructura del Proyecto

```
.
├── main.py                 # Orquestador principal
├── config_loader.py        # Cargador de configuración
├── tebra_client.py         # Cliente Tebra (extracción paralela)
├── bigquery_ops.py         # Operaciones BigQuery
├── sheets_ops.py          # Operaciones Google Sheets
├── transformations.py      # Transformaciones de datos
├── config.yaml            # Configuración
├── requirements.txt       # Dependencias Python
├── Dockerfile             # Imagen Docker
├── cloudbuild.yaml       # CI/CD con Cloud Build
├── deploy.sh             # Script de despliegue
├── setup.sh              # Script de configuración inicial
└── .env.example          # Plantilla de variables de entorno
```

## Prerequisites

- Google Cloud SDK instalado
- Docker instalado
- Cuenta de Google Cloud con billing habilitado

## Configuración Inicial

1. **Clonar el repositorio y crear el archivo .env:**

```bash
cp .env.example .env
```

2. **Editar .env con tus credenciales:**

```env
GCP_PROJECT_ID=your-project-id
BQ_DATASET_ID=your-dataset-id

TEBRA_USER=your-tebra-username
TEBRA_PASSWORD=your-tebra-password
TEBRA_CUSTOMER_KEY=your-customer-key

SHEET_KEY=your-sheet-key
PATIENT_SHEET_KEY=your-patient-sheet-key
INSURANCE_SHEET_KEY=your-insurance-sheet-key
```

3. **Ejecutar script de configuración:**

```bash
chmod +x setup.sh
./setup.sh your-project-id us-central1
```

## Despliegue

### Opción 1: Manual

```bash
chmod +x deploy.sh
./deploy.sh your-project-id us-central1
```

### Opción 2: Automático con Cloud Build

```bash
gcloud builds submit --config=cloudbuild.yaml .
```

## Programación (Cloud Scheduler)

El pipeline se ejecuta automáticamente todos los días a las 6:00 AM:

```bash
# Ver jobs programados
gcloud scheduler jobs list --location=us-central1

# Ejecutar manualmente
gcloud run invoke appointment-pipeline \
    --region=us-central1 \
    --project=your-project-id
```

## Variables de Entorno

| Variable | Descripción | Requerido |
|----------|-------------|-----------|
| `GCP_PROJECT_ID` | ID del proyecto GCP | Sí |
| `BQ_DATASET_ID` | Dataset de BigQuery | Sí |
| `TEBRA_USER` | Usuario de Tebra | Sí |
| `TEBRA_PASSWORD` | Contraseña de Tebra | Sí |
| `TEBRA_CUSTOMER_KEY` | Customer Key de Tebra | Sí |
| `SHEET_KEY` | ID de Google Sheet principal | Sí |
| `PATIENT_SHEET_KEY` | ID de Google Sheet de pacientes | Sí |
| `INSURANCE_SHEET_KEY` | ID de Google Sheet de seguros | Sí |

## Desarrollo Local

```bash
# Crear virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar localmente
python main.py
```

## Logs

```bash
# Ver logs de Cloud Run
gcloud logs read "resource.type=cloud_run_revision" --region=us-central1

# Ver logs en tiempo real
gcloud logs read "resource.type=cloud_run_revision" --region=us-central1 --follow
```

## Permisos Requeridos

- `bigquery.dataEditor`
- `bigquery.jobUser`
- `run.invoker`
- `storage.objectViewer`

## Troubleshooting

### Error de autenticación con Google Sheets
Asegúrate de que el service account tenga acceso a las hojas de cálculo.

### Timeout en Cloud Run
Aumenta el timeout en cloudbuild.yaml o el parámetro `--timeout` en deploy.sh.

### Error de permisos BigQuery
Verifica que el service account tenga los roles necesarios.
