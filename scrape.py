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
#store = kvstore.create('s3://'+s3_bucket)
store = kvstore.create('file://' + os.path.join(os.path.dirname(__file__), 'kvstore'))


def serialise(im):
    output = cStringIO.StringIO()
    im.save(output, format=im.format)
    data = output.getvalue()
    output.close()
    return data


def download_image(url):
    response = requests.get(url, stream=True)
    response.raise_for_status()

    parser = ImageFile.Parser()
    for chunk in response.iter_content(1024):
        parser.feed(chunk)
    im = parser.close()

    io = cStringIO.StringIO()
    im.save(io, format='jpeg')
    return io.getvalue()


def save_image(url, key):
    print('Downloding {}'.format(url))
    im = download_image(url)
    store.put(key, im)


def scrape_webcam(camera):
    url = 'http://www.antarctica.gov.au/webcams/{}'.format(camera)

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    urls = set(map(lambda x: x['href'], soup.find_all('a', href=webcam_re)))

    if not urls:
        urls = set(map(lambda x: x['src'], soup.find_all('img', src=webcam_re)))

        # single cam's are generally every 5 minutes
        # so ensure we got the previous 5 minute image too
        cam_re = re.compile(r'(?P<prefix>/\D)(?P<number>\d+)(?P<postfix>\D\.jpg)$', flags=re.I)
        for image in list(urls):
            try:
                match = cam_re.search(image)
                number = int(match.group('number'))
                prefix, postfix = match.group('prefix'), match.group('postfix')
                previous = cam_re.sub(prefix + str(number - 5) + postfix, image)
                next = cam_re.sub(prefix + str(number - 5) + postfix, image)
                urls.add(previous)
                urls.add(next)
            except:
                pass

    for image in urls:
        try:
            save_image(image, '{}/{}'.format(camera, os.path.basename(image)))
        except:
            print('Failed to download image {}'.format(image))


def run():
    scrape_webcam('aurora')
    scrape_webcam('casey')
    scrape_webcam('davis')
    scrape_webcam('mawson')


def scrape():
    while True:
        run()
        time.sleep(60)


if __name__ == '__main__':
    run()
