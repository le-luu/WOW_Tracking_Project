import pandas as pd
import io

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# Authenticate
flow = InstalledAppFlow.from_client_secrets_file(
    "C:\\Users\\LeLuu\\Documents\\Project\\WOW_Tracking_Project\\client_secret_141692086021-ds5boe7ovsd49tub68k816pd1g9t3n61.apps.googleusercontent.com.json", SCOPES
)
creds = flow.run_local_server(port=0)

service = build("drive", "v3", credentials=creds)

# Example dataframe
df = pd.DataFrame({
    "id":[1,2,3],
    "value":[10,20,30]
})

# Convert dataframe to CSV in memory
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False)

media = MediaIoBaseUpload(
    io.BytesIO(csv_buffer.getvalue().encode()),
    mimetype="text/csv"
)

file_metadata = {
    "name": "wow_data.csv",
    "parents": ["1B_JVeMvC5ZZR68KCrhZCM3bp64T21vmB"]
}

file = service.files().create(
    body=file_metadata,
    media_body=media,
    fields="id"
).execute()

print("Uploaded:", file["id"])