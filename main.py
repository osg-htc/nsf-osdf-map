from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import os


def main():
    load_dotenv()

    client = Elasticsearch(
        hosts=os.getenv("ELASTIC_HOST"),
        basic_auth=(os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD")),
    )

    print(client.info())



if __name__ == "__main__":
    main()
