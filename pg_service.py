from psycopg2 import pool
import os

class DatabaseService:
    def __init__(self):
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.database = os.getenv("POSTGRES_DATABASE")
        self.hostname = os.getenv("POSTGRES_HOST")
        self.pool = pool.SimpleConnectionPool(
            1, 20,
            user=self.user,
            password=self.password,
            database=self.database,
            host=self.hostname,
            port=5432,
            sslmode='require'
        )
        print(f"Connecting to database {self.database} at {self.hostname}...")

    def query(self, sql):
        connection = self.pool.getconn()
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()
        except Exception as e:
            print(f"Database query error: {e}")
            connection.rollback()
            raise e
        finally:
            self.pool.putconn(connection)

    def end(self):
        self.pool.closeall()


class Upgrade:
    def __init__(self, db: DatabaseService):
        self.db = db

    def chunk_array(self, array, size):
        chunks = []
        for i in range(0, len(array), size):
            chunks.append(array[i:i + size])
        return chunks

    def clear_fire_points_table(self):
        sql = "TRUNCATE TABLE fire_points RESTART IDENTITY"
        self.db.query(sql)

    def insert(self, fire_points):
        base_query = """
        INSERT INTO fire_points (
            latitude, longitude, bright_ti4, scan, track,
            acq_date, acq_time, satellite, confidence, version,
            bright_ti5, frp, daynight, ndvi
        ) VALUES """

        chunks = self.chunk_array(fire_points, 100)

        for chunk in chunks:
            values = []
            for point in chunk:
                ndvi_value = point.ndvi if point.ndvi is not None else "NULL"
                values.append(f"({point.latitude}, {point.longitude}, {point.bright_ti4}, {point.scan}, {point.track}, "
                              f"'{point.acq_date}', '{point.acq_time}', '{point.satellite}', '{point.confidence}', "
                              f"'{point.version}', {point.bright_ti5}, {point.frp}, '{point.daynight}', {ndvi_value})")
            full_query = base_query + ", ".join(values)
            self.db.query(full_query)

        print("Successfully update data 🚀")
