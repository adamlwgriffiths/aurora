from __future__ import absolute_import, print_function
import os
import time
import requests
from bs4 import BeautifulSoup
import kvstore
import cStringIO
from PIL import ImageFile

directory = os.path.join(os.path.dirname(__file__), 'imageskvstore')
url = 'http://www.antarctica.gov.au/webcams/aurora'
s3_bucket = 'antarctica-scrape'
store = kvstore.create('s3://'+s3_bucket)
#store = kvstore.create('file://'+directory)


def serialise(im):
    output = cStringIO.StringIO()
    im.save(output, format=im.format)
    data = output.getvalue()
    output.close()
    return data


def download_image(url):
    response = requests.get(url, stream=True)

    parser = ImageFile.Parser()
    for chunk in response.iter_content(1024):
        parser.feed(chunk)
    im = parser.close()

    io = cStringIO.StringIO()
    im.save(io, format='jpeg')
    return io.getvalue()


def save_image(url):
    print('Downloding {}'.format(url))
    filename = os.path.basename(url)
    path = os.path.join(directory, filename)
    try:
        os.makedirs(directory)
    except:
        pass

    im = download_image(url)
    with open(path, 'wb') as f:
        f.write(im)


def scrape_aurora():
    v1, v2, v3 = None, None, None
    while True:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        v12 = soup.find('a', id='view1')['href']
        v22 = soup.find('a', id='view2')['href']
        v32 = soup.find('a', id='view3')['href']
        if v12 != v1:
            v1 = v12
            save_image(v1)
        if v22 != v2:
            v2 = v22
            save_image(v2)
        if v32 != v3:
            v3 = v32
            save_image(v3)
        time.sleep(60)


if __name__ == '__main__':
    scrape_aurora()
