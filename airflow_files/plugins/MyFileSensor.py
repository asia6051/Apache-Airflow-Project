"""
    Custom sensor based on FileSensor
    needed for Module 22 - Airflow Course
"""
from airflow.sensors.filesystem import FileSensor

class MyFileSensor(FileSensor):
    poke_context_fields = ('filepath', 'fs_conn_id')

    def __init__(self, filepath, fs_conn_id='fs_default', *args, **kwargs):
        super().__init__(filepath=filepath, fs_conn_id=fs_conn_id, *args, **kwargs)
