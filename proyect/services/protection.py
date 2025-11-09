from oauth2client.service_account import ServiceAccountCredentials
from proyect.utils.logger import logger
import gspread

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
        Protect the worksheet so only the service account and the owner can edit.
        If protection already exists, fetch and reuse its ID.
        """
        logger.info(f"Applying protection to the '{self.sheet_name}' sheet...")

        service_account_email = self.service_account['client_email']

        body = {
            "requests": [
                {
                    "addProtectedRange": {
                        "protectedRange": {
                            "range": {"sheetId": self.sheet.id},
                            "description": f"Locking the '{self.sheet_name}' sheet",
                            "warningOnly": False,
                            "editors": {"users": [service_account_email, self.owner_email]},
                        }
                    }
                }
            ]
        }

        try:
            response = self.spreadsheet.batch_update(body)
            protected_range = response["replies"][0]["addProtectedRange"]["protectedRange"]
            self._protected_range_id = protected_range["protectedRangeId"]
            logger.info(f"Protection applied. Stored protectedRangeId: {self._protected_range_id}")

        except Exception as e:
            # Caso: ya está protegido o permiso insuficiente
            logger.warning(f"Protection may already exist: {e}")

            # Recuperamos los rangos protegidos actuales
            sheet_metadata = self.spreadsheet.get()
            protections = sheet_metadata.get("sheets", [])[0].get("protectedRanges", [])

            if protections:
                # Buscar el primero que coincida con nuestro sheetId
                for pr in protections:
                    if pr.get("range", {}).get("sheetId") == self.sheet.id:
                        self._protected_range_id = pr.get("protectedRangeId")
                        logger.info(f"Existing protection found. Using protectedRangeId: {self._protected_range_id}")
                        break

            if not getattr(self, "_protected_range_id", None):
                logger.error("No existing protection ID found for this sheet.")
                raise e  # Re-raise si no se pudo recuperar el ID

        except (KeyError, IndexError) as e:
            logger.warning("Could not retrieve protectedRangeId from response. Unprotect may require metadata fetch.")


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