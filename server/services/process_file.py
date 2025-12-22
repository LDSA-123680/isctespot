import os
import pandas as pd
from werkzeug.utils import secure_filename
from db.db_connector import DBConnector

class ProcessFile:
    ''' Calss to process uploaded file '''

    def __init__(self, file, comp_id):
        self.comp_id = comp_id
        self.file = file
        self.dir = os.path.join(os.path.dirname(__file__), 'files')
        self.status = False
        self.file_path = self.save_file()
        self.update_products_from_file(self.file_path)

    def save_file(self) -> str:
        """
        FIX: Path traversal (write) no upload.

        Controles:
          - secure_filename
          - whitelist .csv/.xlsx
          - realpath containment dentro da pasta da empresa
        """
        original_name = self.file.filename or ""
        safe_name = secure_filename(original_name)

        if not safe_name:
            raise ValueError("Invalid filename")

        allowed = {'.csv', '.xlsx'}
        _, ext = os.path.splitext(safe_name.lower())
        if ext not in allowed:
            raise ValueError("Invalid file extension. Allowed: .csv, .xlsx")

        comp_folder = os.path.join(self.dir, str(self.comp_id))
        os.makedirs(comp_folder, exist_ok=True)

        base = os.path.realpath(comp_folder)
        target = os.path.realpath(os.path.join(comp_folder, safe_name))
        if not target.startswith(base + os.sep):
            raise ValueError("Invalid filename (path traversal)")

        self.file.save(target)
        return target

    def update_products_from_file(self, file_path):
        ''' upload database according to the escell data '''
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)

        dbc = DBConnector()
        results = dbc.execute_query(query='update_products_by_comp_id', args={'file': df, 'comp_id': self.comp_id})
        if results is True:
            self.is_updated = True
            print("Products updated successfully.")
        else:
            self.is_updated = False
