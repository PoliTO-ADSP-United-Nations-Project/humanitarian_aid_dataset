import os

import requests
from zipfile import ZipFile


class FigshareUtils:

    @staticmethod
    def download_dataset(url: str, path: str, file_name: str = "undefined") -> bool:
        print(f'Starting download {file_name} file from figshare')
        if not os.path.exists(path):
            os.makedirs(path)
        if os.path.exists(os.path.join(path, file_name)):
            print(f'Dataset "{file_name}" already exists')
            return True
        res = requests.get(url)
        if res.status_code == 200:
            open(os.path.join(path, file_name), "wb").write(res.content)
            return True
        return False

    @staticmethod
    def download_datasets(url: str, path: str) -> bool:
        print(f'Starting download the zip file from figshare')
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            print(f'Folder "{path}" is not empty so dataset already exists')
            return True
        res = requests.get(url)
        if res.status_code == 200:
            open(os.path.join(path, 'tmp.zip'), "wb").write(res.content)
            # Unzip file
            with ZipFile(os.path.join(path, 'tmp.zip'), 'r') as zObject:
                zObject.extractall(path=path)
                os.remove(os.path.join(path, 'tmp.zip'))
                return True
        return False
