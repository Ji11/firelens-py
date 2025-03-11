from dotenv import load_dotenv
from process import Process
from pg_service import DatabaseService, Upgrade
from download import Download
import schedule
import time

load_dotenv()

def upgrade_fire_points_data():
    downloader = Download()
    processor = Process()
    db = DatabaseService()
    upgrader = Upgrade(db)

    try:
        downloader.download()
        records = processor.process()
        upgrader.clear_fire_points_table()
        upgrader.insert(records)
    except Exception as e:
        print(f"System Error: {str(e)}")
    finally:
        db.end()



if __name__ == "__main__":
    try:
        upgrade_fire_points_data()
        schedule.every(60).minutes.do(upgrade_fire_points_data)

        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Update stopped")
    except Exception as e:
        print(f"An error occurred: {str(e)}")