from oauth2client.service_account import ServiceAccountCredentials
from proyect.utils.logger import logger
import gspread

class SheetProtector:
    """
    Class to protect or unprotect a specific worksheet in a Google Sheet.
    """

    def __init__(self, service_account=None, scopes=None, spreadsheet_id=None, sheet_name="urls"):
        self.service_account = service_account
        self.scopes = scopes
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name

        logger.info("Authorizing service account...")
        creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account, scopes)
        self.client = gspread.authorize(creds)

        logger.info(f"Opening spreadsheet: {spreadsheet_id}")
        self.spreadsheet = self.client.open_by_key(spreadsheet_id)
        self.sheet = self.spreadsheet.worksheet(sheet_name)

    def protect(self):
        """Protect the worksheet so only the service account and the owner can edit."""
        logger.info(f"Applying protection to the '{self.sheet_name}' sheet...")

        # Service account email
        service_account_email = self.service_account['client_email']
        # Human owner email
        owner_email = "guiaslocalesar@gmail.com"

        body = {
            "requests": [
                {
                    "addProtectedRange": {
                        "protectedRange": {
                            "range": {"sheetId": self.sheet.id},
                            "description": f"Locking the '{self.sheet_name}' sheet",
                            "warningOnly": False,
                            "editors": {"users": [service_account_email, owner_email]}
                        }
                    }
                }
            ]
        }

        self.spreadsheet.batch_update(body)
        logger.info(f"The '{self.sheet_name}' sheet is now protected. Only owner and service account can edit.")


    def unprotect(self):
        """Remove all protections from the worksheet."""
        logger.info(f"Fetching protections on the '{self.sheet_name}' sheet...")
        protections = self.sheet.protected_ranges

        if not protections:
            logger.info("No protections found. Nothing to remove.")
            return

        logger.info(f"Removing {len(protections)} protection(s)...")
        requests = [{"deleteProtectedRange": {"protectedRangeId": p.id}} for p in protections]
        self.spreadsheet.batch_update({"requests": requests})
        logger.info(f"The '{self.sheet_name}' sheet is now unprotected.")
