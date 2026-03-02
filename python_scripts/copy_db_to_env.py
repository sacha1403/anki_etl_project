import shutil
import logging
from dotenv import load_dotenv
import os

def copy_db():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        load_dotenv()
        src = os.getenv("ANKI_DB_ORIGIN")
        dst = os.getenv("ANKI_DB_PATH")
        shutil.copyfile(src, dst)
        return
    except Exception as e:
        logging.error(f"Error copying anki database into local folder: {e}")
        raise