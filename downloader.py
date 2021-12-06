import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import os
import json

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()])


def remove_control(line):
    return ''.join(c for c in line if ord(c) >= 32)


def is_absolute(url):
    return bool(urlparse(url).netloc)


def download_url(url):
    return requests.get(url).text


def get_linked_urls(url, html):
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all('a'):
        path = link.get('href')
        if not is_absolute(path):
            path = urljoin(url, path)

        size = remove_control(str(link.nextSibling)).split(' ')[-1]
        yield path, size


def get_domains(accepted_domains, urls):
    if len(accepted_domains) == 0:
        for url in urls:
            domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(url))
            accepted_domains.append(domain)
    return accepted_domains


class Crawler:
    def __init__(self, urls=[], accepted_domains=[], download_folder = '/'):
        self.visited_urls = []
        self.urls_to_visit = urls
        self.sizes = dict()
        self.accepted_domains = get_domains(accepted_domains, urls)
        self.downloaded_links = []
        self.download_folder = download_folder

    def add_url_to_visit(self, url, size):
        domain ='{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(url))

        if url not in self.visited_urls and url not in self.urls_to_visit and domain in self.accepted_domains:
            self.urls_to_visit.append(url)
            self.sizes[url] = size

    def crawl(self, url):
        html = download_url(url)
        for url, size in get_linked_urls(url, html):
            self.add_url_to_visit(url, size)

    def run(self):
        while self.urls_to_visit:
            url = self.urls_to_visit.pop(0)
            r = requests.head(url)

            if "text/html" in r.headers["content-type"]:
                logging.info(f'Crawling: {url}')
                try:
                    self.crawl(url)
                except Exception as e:
                    logging.exception(f'Failed to crawl: {url}; with exception: {e}')
                finally:
                    self.visited_urls.append(url)
            else:

                if url not in self.downloaded_links and url not in self.urls_to_visit:
                    logging.info(f'Downloading: {url}')
                    parsed_url = urlparse(url)

                    download_loc = self.download_folder + parsed_url.path

                    try:
                        f_size = str(os.path.getsize(download_loc))
                    except Exception:
                        f_size = 'Doesn\'t exist'
                    finally:
                        logging.info(f'Local size: {f_size}; Server size: {self.sizes.get(url)}')

                    if not os.path.isfile(download_loc) or f_size != self.sizes.get(url):
                        if not os.path.exists(dir_path := os.path.dirname(download_loc)):
                            os.makedirs(dir_path)
                            os.chmod(dir_path, 666)

                        req = requests.get(url)
                        open(download_loc, 'wb').write(req.content)
                        logging.info(f'Finished downloading: {url}')

                        self.downloaded_links.append(url)
                    else:
                        logging.info(f'File already exists: {url}')

if __name__ == '__main__':
    config = json.load(open('config.json'))

    Crawler(urls=config["urls"],
            accepted_domains=config["accepted_domains"],
            download_folder=config["download_folder"]).run()