import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging


logger = logging.getLogger(__name__)


class TebraClient:
    def __init__(self, client, request_header: dict):
        self.client = client
        self.request_header = request_header

    def _build_request(self, start_date: str) -> dict:
        return {
            'RequestHeader': self.request_header,
            'Filter': {'StartDate': start_date},
            'Fields': {
                'ID': True,
                'ConfirmationStatus': True,
                'ServiceLocationName': True,
                'PatientID': True,
                'PatientFullName': True,
                'PatientCaseID': True,
                'PatientCaseName': True,
                'PatientCasePayerScenario': True,
                'StartDate': True,
                'EndDate': True,
                'AppointmentReason1': True,
                'ResourceName1': True,
                'CreatedDate': True,
                'LastModifiedDate': True
            }
        }

    def _fetch_single_date(self, date: str) -> list:
        request = self._build_request(date)
        response = self.client.service.GetAppointments(request)
        
        appointments = []
        try:
            appointment_data = getattr(getattr(response, 'Appointments', None), 'AppointmentData', [])
            for apt in appointment_data:
                appointments.append({
                    'ID': apt.ID,
                    'ConfirmationStatus': apt.ConfirmationStatus,
                    'ServiceLocationName': apt.ServiceLocationName,
                    'PatientID': apt.PatientID,
                    'PatientFullName': apt.PatientFullName,
                    'PatientCaseID': apt.PatientCaseID,
                    'PatientCaseName': apt.PatientCaseName,
                    'PatientCasePayerScenario': apt.PatientCasePayerScenario,
                    'StartDate': apt.StartDate,
                    'EndDate': apt.EndDate,
                    'CreatedDate': apt.CreatedDate,
                    'LastModifiedDate': apt.LastModifiedDate,
                    'AppointmentReason1': apt.AppointmentReason1,
                    'Provider': apt.ResourceName1,
                })
        except Exception as e:
            logger.exception("Error parsing Tebra appointments for %s: %s", date, e)
            raise
        return appointments

    def fetch_appointments_parallel(
        self,
        dates: list,
        max_workers: int = 10
    ) -> pd.DataFrame:
        all_appointments = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {
                executor.submit(self._fetch_single_date, date): date 
                for date in dates
            }
            
            completed = 0
            for future in as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    appointments = future.result()
                    all_appointments.extend(appointments)
                    completed += 1
                    if completed % 20 == 0:
                        logger.info("Procesadas %s/%s fechas", completed, len(dates))
                except Exception as e:
                    logger.exception("Error extrayendo fecha %s: %s", date, e)
                    raise
        
        logger.info("Total registros extraídos: %s", len(all_appointments))
        return pd.DataFrame(all_appointments)

    def fetch_appointments_sequential(self, dates: list) -> pd.DataFrame:
        all_appointments = []
        for i, date in enumerate(dates):
            appointments = self._fetch_single_date(date)
            all_appointments.extend(appointments)
            if (i + 1) % 20 == 0:
                logger.info("Procesadas %s/%s fechas", i + 1, len(dates))
        logger.info("Total registros extraídos: %s", len(all_appointments))
        return pd.DataFrame(all_appointments)
