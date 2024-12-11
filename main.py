from src.table_creator import create_table
from src.fetcher import fetch_data
from src.preprocessor import preprocess_data
from src.uploader import upload_data


def main():
    create_table()
    fetch_data()
    preprocess_data()
    upload_data()


if __name__ == "__main__":
    main()