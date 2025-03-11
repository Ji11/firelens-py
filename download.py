import requests
import time
import os

class Download:
    def __init__(self):
        self.url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/csv/J2_VIIRS_C2_Global_48h.csv"
        self.output = "./data/source_data.csv"
        self.timeout = 300000
    
    def download(self):
        try:
            start_time = time.time()
            response = requests.get(self.url, stream = True, timeout=self.timeout)

            if response.status_code != 200:
                raise Exception(f"Download failed, status code: {response.status_code} {response.reason}")
            
            os.makedirs(os.path.dirname(self.output), exist_ok=True)

            with open(self.output, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        used_time = time.time() - start_time
                        if used_time > self.timeout:
                            raise Exception("Download timeout")
                        file.write(chunk)

            print("\nSuccessfully download source data 🌏")
        
        except Exception as error:
            print("Download failed: ", error)
            raise error

