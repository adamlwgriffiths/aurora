from __future__ import absolute_import, print_function
import re
import os
import time
import datetime
import requests
from bs4 import BeautifulSoup
import kvstore
import cStringIO
from PIL import ImageFile
from multiprocessing import Pool


webcam_re = re.compile(r'/webcams/.*\.jpg', flags=re.I)

s3_bucket = 'antarctica-scrape'
if os.environ.get('SCRAPE_STORE') == 's3' and os.environ.get('AWS_ACCESS_KEY_ID', None):
    store = kvstore.create('s3://'+s3_bucket)
else:
    store = kvstore.create('file://' + os.path.join(os.path.dirname(__file__), 'kvstore'))

_pool = None


def pool():
    global _pool
    if not _pool:
        _pool = Pool(5)
    return _pool


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


def day_difference(d1, d2):
    absd1 = datetime.datetime(year=d1.year, month=d1.month, day=d1.day)
    absd2 = datetime.datetime(year=d2.year, month=d2.month, day=d2.day)
    return (absd1 - absd2).days


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


def historic_aurora(image):
    base = 'aurora'
    base_code = image[0]
    image_day = int(image[1:-4])
    image_hour = int(image[-4:-2])
    image_minute = int(image[-2:])

    now = datetime.datetime.now()
    t_orig = t = datetime.datetime(now.year, now.month, now.day, image_hour, image_minute)

    # the first image i managed to capture
    cameras = ['A', 'B', 'C']

    min_t = datetime.datetime(2015, 10, 29)
    while t > min_t:
        t_new = t

        for camera in cameras:
            # the camera sometimes gets images that are after the 30m marks
            # so poke about +10 minutes
            for x in range(10):
                try:
                    day_code = image_day - day_difference(t_orig, t_new)

                    image = 'http://images.antarctica.gov.au/webcams/{base}/15/{code}{number}{time}{camera}.jpg'.format(
                        base=base,
                        code=base_code,
                        number=day_code,
                        time=t_new.strftime('%H%M'),
                        camera=camera,
                    )
                    save_image(image, '{}/{}'.format(base, os.path.basename(image)))
                    break
                except:
                    print('Failed to download image {}'.format(image))
                    t_new += datetime.timedelta(minutes=1)

        t_new = t - datetime.timedelta(minutes=30)
        t = t_new


def historic_base_urls(base, path):
    year, month, day, image = path.split('/')
    base_code = image[0]

    # C1511302355s
    # base + year + month + day + hour + min + s
    timestamp = image[1:]
    year = int('20'+timestamp[0:2])
    month = int(timestamp[2:4])
    day = int(timestamp[4:6])
    hour = int(timestamp[6:8])
    minute = int(timestamp[8:])

    t = datetime.datetime(year, month, day, hour, minute)

    min_t = datetime.datetime(2015, 10, 29)
    while t > min_t:
        image = 'http://images.antarctica.gov.au/webcams/{base}/{year}/{month}/{day}/{base_code}{year_short}{month}{day}{hour}{minute}{camera}.jpg'.format(
            base=base,
            base_code=base_code,
            year=t.strftime('%Y'),
            year_short=t.strftime('%y'),
            month=t.strftime('%m'),
            day=t.strftime('%d'),
            hour=t.strftime('%H'),
            minute=t.strftime('%M'),
            camera='s'
        )
        key = '{}/{}'.format(base, os.path.basename(image))
        yield image, key

        t_new = t - datetime.timedelta(minutes=5)
        t = t_new


def save_image_star(args):
    try:
        save_image(*args)
    except:
        print('Failed to download image {}'.format(args[0]))


def historic_base(base, path):
    args = historic_base_urls(base, path)
    args = filter(lambda (image, key): not store.exists(key), args)
    pool().map(save_image_star, args)


def run():
    scrape_webcam('aurora')
    scrape_webcam('casey')
    scrape_webcam('davis')
    scrape_webcam('mawson')


def scrape():
    while True:
        run()
        time.sleep(60)


def historic():
    aurora = 'A153360730'
    casey = '2015/12/02/C1512020730'
    davis = '2015/12/02/D1512020735'
    mawson = '2015/12/02/M1512020730'

    #historic_aurora(aurora)
    historic_base('casey', casey)
    historic_base('davis', davis)
    historic_base('mawson', mawson)


if __name__ == '__main__':
    if os.environ.get('SCRAPE_MODE') == 'historic':
        historic()
    else:
        run()
