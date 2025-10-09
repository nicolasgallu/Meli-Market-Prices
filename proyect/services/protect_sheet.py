from oauth2client.service_account import ServiceAccountCredentials
from proyect.utils.logger import logger
import gspread

class SheetProtector:
    def __init__(self, service_account, scopes, spreadsheet_id, sheet_name="Scrapping"):
        """
        Initialize the SheetProtector.

        Args:
            service_account (dict): Service account credentials.
            scopes (list): Google API scopes.
            spreadsheet_id (str): ID of the Google Spreadsheet.
            sheet_name (str): Name of the worksheet to protect. Defaults to "Scrapping".
        """
        self.service_account = service_account
        self.scopes = scopes
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.protected_range_id = None

        # Connect to Google Sheets
        creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account, scopes)
        client = gspread.authorize(creds)
        self.spreadsheet = client.open_by_key(spreadsheet_id)
        self.sheet = self.spreadsheet.worksheet(sheet_name)
        self.service_email = service_account.get("client_email")

    def block(self, description="Locked during data update"):
        """
        Lock the worksheet so that only the service account can edit.
        """
        body_add_lock = {
            "requests": [
                {
                    "addProtectedRange": {
                        "protectedRange": {
                            "range": {"sheetId": self.sheet.id},
                            "description": description,
                            "warningOnly": False,
                            "editors": {"users": [self.service_email]}
                        }
                    }
                }
            ]
        }
        response = self.spreadsheet.batch_update(body_add_lock)
        self.protected_range_id = response['replies'][0]['addProtectedRange']['protectedRange']['protectedRangeId']
        logger.info("Blocked Sheet.")

    def unblock(self):
        """
        Remove the protection from the worksheet.
        """
        if not self.protected_range_id:
            return  # nothing to remove

        body_remove_lock = {
            "requests": [
                {
                    "deleteProtectedRange": {
                        "protectedRangeId": self.protected_range_id
                    }
                }
            ]
        }
        self.spreadsheet.batch_update(body_remove_lock)
        logger.info("Unblocked Sheet.")
