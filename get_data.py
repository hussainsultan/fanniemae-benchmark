import wget

URL = "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2016.tgz"


def main():
    wget.download(URL, "data/")


if __name__ == "__main__":
    main()
