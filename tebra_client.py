import pandas as pd
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


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
            for apt in response.Appointments.AppointmentData:
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
            print(f"Error fetching {date}: {e}")
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
                        print(f"Procesadas {completed}/{len(dates)} fechas")
                except Exception as e:
                    print(f"Error en fecha {date}: {e}")
        
        print(f"Total registros extraídos: {len(all_appointments)}")
        return pd.DataFrame(all_appointments)

    def fetch_appointments_sequential(self, dates: list) -> pd.DataFrame:
        all_appointments = []
        for i, date in enumerate(dates):
            appointments = self._fetch_single_date(date)
            all_appointments.extend(appointments)
            if (i + 1) % 20 == 0:
                print(f"Procesadas {i + 1}/{len(dates)} fechas")
        print(f"Total registros extraídos: {len(all_appointments)}")
        return pd.DataFrame(all_appointments)
