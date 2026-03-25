# Appointment Pipeline - Cloud Run Jobs Deployment

Pipeline automatizado para extraer citas de Tebra, procesarlas y sincronizarlas con BigQuery y Google Sheets.
El despliegue recomendado es como **Cloud Run Job** ejecutado por **Cloud Scheduler**.

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
├── cloudbuild.yaml       # Build + update de imagen del Cloud Run Job
├── cloudrun.env.example.yaml # Plantilla de variables para el job
├── deploy.sh             # Script de despliegue manual del job
├── setup.sh              # Script de configuración inicial GCP
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

### Opción 1: Bootstrap manual

```bash
cp cloudrun.env.example.yaml cloudrun.env.yaml
chmod +x deploy.sh
./deploy.sh your-project-id us-central1
```

Esto crea o actualiza el job con su configuración completa.
La plantilla `cloudrun.env.example.yaml` incluye las variables OAuth que hoy necesita
la app en Cloud Run para autenticarse con Google Sheets y BigQuery.

### Opción 2: Automático con Cloud Build

```bash
gcloud builds submit --config=cloudbuild.yaml .
```

Esta opción está pensada para **actualizar la imagen** de un job que ya existe.
No redefine variables ni secretos del runtime.

### Windows PowerShell

```powershell
Copy-Item cloudrun.env.example.yaml cloudrun.env.yaml
.\setup.ps1 -ProjectId "your-project-id" -Region "us-central1"
.\deploy.ps1 -ProjectId "your-project-id" -Region "us-central1"
```

## Programación (Cloud Scheduler)

El pipeline debe ejecutarse mediante Cloud Scheduler llamando al endpoint `jobs.run`
de Cloud Run Jobs. El script `deploy.sh` ya crea o actualiza ese scheduler.

```bash
# Ver jobs de scheduler
gcloud scheduler jobs list --location=us-central1

# Ejecutar manualmente el job
gcloud run jobs execute appointment-pipeline \
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
| `CLIENT_ID` | OAuth Client ID de Google | Sí en Cloud Run |
| `CLIENT_SECRET` | OAuth Client Secret de Google | Sí en Cloud Run |
| `REFRESH_TOKEN` | Refresh token con scopes de Sheets/Drive/BigQuery | Sí en Cloud Run |
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
# Ver logs del job
gcloud logs read "resource.type=cloud_run_job" --project=your-project-id

# Ver logs en tiempo real
gcloud logs read "resource.type=cloud_run_job" --project=your-project-id --follow
```

## Permisos Requeridos

- `bigquery.dataEditor`
- `bigquery.jobUser`
- `run.invoker`
- `storage.objectViewer`

## Troubleshooting

### Error de autenticación con Google Sheets
Asegúrate de que el service account tenga acceso a las hojas de cálculo.

### Timeout en Cloud Run Jobs
Aumenta el timeout en `cloudbuild.yaml` o el parámetro `--timeout` en `deploy.sh`.

### Error de permisos BigQuery
Verifica que el service account tenga los roles necesarios.
