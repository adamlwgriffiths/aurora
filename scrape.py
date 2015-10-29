from __future__ import absolute_import, print_function
import re
import os
import time
import requests
from bs4 import BeautifulSoup
import kvstore
import cStringIO
from PIL import ImageFile

webcam_re = re.compile(r'/webcams/.*\.jpg', flags=re.I)

s3_bucket = 'antarctica-scrape'
store = kvstore.create('s3://'+s3_bucket)
#store = kvstore.create('file://' + os.path.join(os.path.dirname(__file__), 'kvstore'))


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


def save_image(url, key):
    print('Downloding {}'.format(url))
    #filename = os.path.basename(url)
    #path = os.path.join(directory, filename)

    im = download_image(url)
    store.put(key, im)
    #with open(path, 'wb') as f:
    #    f.write(im)


aurora_1 = None
aurora_2 = None
aurora_3 = None
aurora_directory = 'aurora/'


def scrape_aurora():
    url = 'http://www.antarctica.gov.au/webcams/aurora'
    global aurora_1
    global aurora_2
    global aurora_3

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    a1 = soup.find('a', id='view1')['href']
    a2 = soup.find('a', id='view2')['href']
    a3 = soup.find('a', id='view3')['href']
    if a1 != aurora_1:
        aurora_1 = a1
        save_image(a1, aurora_directory + os.path.basename(a1))
    if a2 != aurora_2:
        aurora_2 = a2
        save_image(a2, aurora_directory + os.path.basename(a2))
    if a3 != aurora_3:
        aurora_3 = a3
        save_image(a3, aurora_directory + os.path.basename(a3))

casey = None
casey_directory = 'casey/'
def scrape_casey():
    url = 'http://www.antarctica.gov.au/webcams/casey'
    global casey

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    img = soup.find('img', src=webcam_re)['src']
    if img != casey:
        casey = img
        save_image(img, casey_directory + os.path.basename(img))

davis = None
davis_directory = 'davis/'
def scrape_davis():
    url = 'http://www.antarctica.gov.au/webcams/davis'
    global davis

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    img = soup.find('img', src=webcam_re)['src']
    if img != davis:
        davis = img
        save_image(img, davis_directory + os.path.basename(img))

mawson = None
mawson_directory = 'mawson/'
def scrape_mawson():
    url = 'http://www.antarctica.gov.au/webcams/mawson'
    global mawson

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    img = soup.find('img', src=webcam_re)['src']
    if img != mawson:
        mawson = img
        save_image(img, mawson_directory + os.path.basename(img))


def run():
    scrape_aurora()
    scrape_casey()
    scrape_davis()
    scrape_mawson()


def scrape():
    while True:
        run()
        time.sleep(60)


if __name__ == '__main__':
    run()
