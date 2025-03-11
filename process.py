import csv
import os
import rasterio

class FirePoint:
    def __init__(self, latitude, longitude, bright_ti4, scan, track, acq_date, acq_time, satellite, confidence, version,
                 bright_ti5, frp, daynight, ndvi=None):
        self.latitude = latitude
        self.longitude = longitude
        self.bright_ti4 = bright_ti4
        self.scan = scan
        self.track = track
        self.acq_date = acq_date
        self.acq_time = acq_time
        self.satellite = satellite
        self.confidence = confidence
        self.version = version
        self.bright_ti5 = bright_ti5
        self.frp = frp
        self.daynight = daynight
        self.ndvi = ndvi

class Process:
    def __init__(self):
        self.input = "./data/source_data.csv"
        self.output = "./data/process_data.csv"
        self.tif = "./data/ndvi2407.tif"
        self.tif_data_cache = None

    def process(self):
        try:
            self.check_files()
            tif_data = self.load_tif()
            get_ndvi = self.create_calculator(tif_data)

            with open(self.input, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                records = list(reader)

            total_records = len(records)
            results = []

            for data in records:
                lat = float(data['latitude'])
                lon = float(data['longitude'])

                processed_data = {
                    'latitude': lat,
                    'longitude': lon,
                    'bright_ti4': float(data['bright_ti4']),
                    'scan': float(data['scan']),
                    'track': float(data['track']),
                    'acq_date': data['acq_date'],
                    'acq_time': data['acq_time'],
                    'satellite': data['satellite'],
                    'confidence': data['confidence'],
                    'version': data['version'],
                    'bright_ti5': float(data['bright_ti5']),
                    'frp': float(data['frp']),
                    'daynight': data['daynight']
                }

                try:
                    ndvi_value = get_ndvi(lat, lon)
                except Exception as e:
                    print(f"Calculator ndvi error ({lat}, {lon}): {str(e)}")
                    ndvi_value = None

                processed_data['ndvi'] = ndvi_value
                results.append(processed_data)

            headers = ['latitude', 'longitude', 'bright_ti4', 'scan', 
                       'track', 'acq_date', 'acq_time', 'satellite',
                       'confidence', 'version', 'bright_ti5', 'frp', 'daynight', 'ndvi']

            with open(self.output, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                writer.writerows(results)

            print(f"Successfully process data, total records: {total_records} 🌲")
            return [FirePoint(**result) for result in results]

        except Exception as e:
            print(f"Process error: {str(e)}")
            raise e

    def check_files(self):
        if not os.path.exists(self.input) or not os.path.exists(self.tif):
            raise FileNotFoundError("input path error")

    def load_tif(self):
        if self.tif_data_cache:
            return self.tif_data_cache

        with rasterio.open(self.tif) as src:
            rasters = src.read()
            bbox = src.bounds
            width = src.width
            height = src.height

            result = {
                'rasters': rasters,
                'bbox': bbox,
                'width': width,
                'height': height
            }

            self.tif_data_cache = result
            return result

    def create_calculator(self, tif_data):
        rasters = tif_data['rasters']
        bbox = tif_data['bbox']
        width = tif_data['width']
        height = tif_data['height']
        x_min, y_min, x_max, y_max = bbox

        def get_ndvi(lat, lon):
            if lon < x_min or lon > x_max or lat < y_min or lat > y_max:
                return None

            x = ((lon - x_min) / (x_max - x_min)) * width
            y = ((y_max - lat) / (y_max - y_min)) * height
            ix = int(x)
            iy = int(y)

            if ix < 0 or ix >= width or iy < 0 or iy >= height:
                return None

            return rasters[0, iy, ix]

        return get_ndvi