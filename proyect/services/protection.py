from oauth2client.service_account import ServiceAccountCredentials
from proyect.utils.logger import logger
import gspread
from gspread.exceptions import APIError

class SheetProtector:
    """
    Class to protect or unprotect a specific worksheet in a Google Sheet.
    """

    def __init__(self, service_account=None, scopes=None, spreadsheet_id=None, sheet_name=None, owner_email=None):
        self.service_account = service_account
        self.scopes = scopes
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.owner_email = owner_email

        logger.info("Authorizing service account...")
        creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account, scopes)
        self.client = gspread.authorize(creds)

        logger.info(f"Opening spreadsheet: {spreadsheet_id}")
        self.spreadsheet = self.client.open_by_key(spreadsheet_id)
        self.sheet = self.spreadsheet.worksheet(sheet_name)
        
    def protect(self):
        """
        Add protection to the worksheet if it doesn't exist.
        If protection already exists, just store its ID.
        """
        sheet_name = self.sheet_name
        worksheet = self.spreadsheet.worksheet(sheet_name)

        try:
            # 1️⃣ Obtener todas las protecciones del spreadsheet
            protections = self.spreadsheet.fetch_sheet_metadata()["sheets"]

            existing_protection_id = None

            # 2️⃣ Buscar si ya hay una protección para esta hoja
            for s in protections:
                if s["properties"]["title"] == sheet_name:
                    protections_list = s.get("protectedRanges", [])
                    if protections_list:
                        existing_protection_id = protections_list[0]["protectedRangeId"]
                    break

            # 3️⃣ Si ya existe, guardamos el ID y salimos
            if existing_protection_id:
                self._protected_range_id = existing_protection_id
                logger.warning(
                    f"Protection already exists for sheet '{sheet_name}'. Using ID {existing_protection_id}."
                )
                return

            # 4️⃣ Si no existe, la creamos
            body = {
                "requests": [
                    {
                        "addProtectedRange": {
                            "protectedRange": {
                                "range": {"sheetId": worksheet.id},
                                "description": "Sheet protection added by script",
                                "warningOnly": False
                            }
                        }
                    }
                ]
            }

            response = self.spreadsheet.batch_update(body)
            added_protection_id = (
                response.get("replies", [{}])[0]
                .get("addProtectedRange", {})
                .get("protectedRange", {})
                .get("protectedRangeId")
            )

            if added_protection_id:
                self._protected_range_id = added_protection_id
                logger.info(f"Protection created for '{sheet_name}' with ID {added_protection_id}.")
            else:
                logger.warning(f"Protection created for '{sheet_name}', but ID not found in response.")

        except APIError as e:
            if "already has sheet protection" in str(e):
                logger.warning(f"Protection already exists for '{sheet_name}': {e}")
            else:
                raise

    def unprotect(self):
        """
        Remove protection from the worksheet using the stored protection ID.
        If the ID is not available, raises an error.
        """

        logger.info(f"Removing protection from the '{self.sheet_name}' sheet using stored ID...")

        body = {
            "requests": [
                {"deleteProtectedRange": {"protectedRangeId": self._protected_range_id}}
            ]
        }

        self.spreadsheet.batch_update(body)
        logger.info(f"The '{self.sheet_name}' sheet is now unprotected.")
        del self._protected_range_id