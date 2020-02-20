import os
import zipfile
import requests
from tqdm import tqdm
import config as cfg
from preprocess_data import split_data_to_chunks


def download_data(zip_name):
    r = requests.get(cfg.DATA_URL, stream=True)
    total_size = int(r.headers.get('content-length', 0))
    block_size = 1024 * 1024 * 4
    t = tqdm(total=total_size, unit='B', unit_scale=True)
    print('Downloading data')
    with open(zip_name, 'wb') as f:
        for data in r.iter_content(block_size):
            t.update(len(data))
            f.write(data)
    t.close()
    if total_size != 0 and t.n != total_size:
        raise NameError("ERROR, something went wrong with downloading zip file")
    else:
        print('Download completed')


def load_data():
    extracted_data_dir = cfg.ROW_DATA_DIR
    if not os.path.exists(extracted_data_dir):
        zip_name = '../temp/data.zip'
        if not os.path.exists(zip_name):
            if not os.path.exists('../temp'):
                os.mkdir('../temp')
            download_data(zip_name)
        else:
            print('Zip file already downloaded')
        print('Extracting')
        z = zipfile.ZipFile(zip_name)
        files = [name for name in z.namelist() if name.startswith('data/')]
        for file in tqdm(files):
            z.extract(file, "../data/")
        os.rename("../data/data/", extracted_data_dir)
    else:
        print('Raw data already downloaded')


def split_to_chunks():
    purchases_csv_path = cfg.PURCHASE_CSV_PATH
    output_jsons_dir = cfg.JSONS_DIR

    if os.path.exists(purchases_csv_path):
        split_data_to_chunks(purchases_csv_path, output_jsons_dir, n_shards=8)
    else:
        raise NameError('Not enough data')
