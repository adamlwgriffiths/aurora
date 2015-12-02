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

webcam_re = re.compile(r'/webcams/.*\.jpg', flags=re.I)

s3_bucket = 'antarctica-scrape'
if os.environ.get('STORE', 's3') is 's3' and os.environ.get('AWS_ACCESS_KEY_ID', None):
    store = kvstore.create('s3://'+s3_bucket)
else:
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
        #if t_new.day != t.day:
        #    image_day -= 1
        t = t_new


def historic_base(base, path):
    year, month, day, image = path.split('/')
    base_code = image[0]
    image_day = int(image[1:-4])
    image_hour = int(image[-4:-2])
    image_minute = int(image[-2:])

    t_orig = t = datetime.datetime(int(year), int(month), int(day), image_hour, image_minute)

    min_t = datetime.datetime(2015, 10, 29)
    while t > min_t:
        day_code = image_day - day_difference(t_orig, t)
        image = 'http://images.antarctica.gov.au/webcams/{base}/{date}/{code}{number}{time}s.jpg'.format(
            base=base,
            date=t.strftime('%Y/%m/%d'),
            code=base_code,
            number=day_code,
            time=t.strftime('%H%M'),
        )
        try:
            save_image(image, '{}/{}'.format(base, os.path.basename(image)))
        except:
            print('Failed to download image {}'.format(image))
        t_new = t - datetime.timedelta(minutes=5)
        #if t_new.day != t.day:
        #    image_day -= 1
        t = t_new


#Downloding http://images.antarctica.gov.au/webcams/aurora/15/A153360730C.jpg
#Downloding http://images.antarctica.gov.au/webcams/aurora/15/A153360730A.jpg
#Downloding http://images.antarctica.gov.au/webcams/aurora/15/A153360730B.jpg
#Downloding http://images.antarctica.gov.au/webcams/casey/2015/12/02/C1512020730s.jpg
#Downloding http://images.antarctica.gov.au/webcams/davis/2015/12/02/D1512020735s.jpg
#Downloding http://images.antarctica.gov.au/webcams/mawson/2015/12/02/M1512020730s.jpg

def historic_bases():
    casey = '2015/12/02/C1512020730'
    davis = '2015/12/02/D1512020735'
    mawson = '2015/12/02/M1512020730'

    #casey = '2015/12/02/C1512020005'

    historic_base('casey', casey)
    historic_base('davis', davis)
    historic_base('mawson', mawson)


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
    #run()
    historic_aurora('A153360730')
    historic_bases()
