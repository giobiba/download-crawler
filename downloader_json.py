import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import os
import json
from http.cookies import SimpleCookie
from datetime import datetime
from pathlib import Path
from requests_html import HTMLSession
import requests

from selenium import webdriver
from selenium.webdriver.common.keys import Keys


def remove_control(line):
    return ''.join(c for c in line if ord(c) >= 32)


def is_absolute(url):
    return bool(urlparse(url).netloc)


def get_cookie(rawdata):
    cookie = SimpleCookie()
    cookie.load(rawdata)
    return {key: value.value for key, value in cookie.items()}


def get_domains(accepted_domains, urls):
    if len(accepted_domains) == 0:
        for url in urls:
            domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(url))
            accepted_domains.append(domain)
    return accepted_domains


class Crawler:
    def __init__(self, urls=[], accepted_domains=[], download_folder='/', verify=True, username='',password='', login=True, login_url='', download_url_path=''):
        self.visited_urls = []
        self.sizes = dict()
        self.is_folder = dict()
        self.urls_to_visit = urls
        self.accepted_domains = get_domains(accepted_domains, urls)
        self.download_url_path = download_url_path
        self.downloaded_links = []
        self.download_folder = download_folder
        self.verify = verify
        self.session = requests.session()
        self.head_headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'close'
            }

        if login:
            login = '{\
                    "user": "' + username + '",\
                    "password": "' + password + '",\
                    "type": "login"\
                }'
            headers = {
                'Content-Type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            }
            res = self.session.post(login_url, headers=headers, data=login)
            self.cookies = res.cookies

        else:
            self.body = ''
            self.headers = ''

    def download_url(self, url):
        res = self.session.get(url, verify=self.verify, cookies=self.cookies)
        self.cookies = res.cookies

        return res.text, res.status_code

    def add_url_to_visit(self, url, size, is_folder):
        domain ='{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(url))

        if url not in self.visited_urls and url not in self.urls_to_visit and domain in self.accepted_domains:
            self.urls_to_visit.append(url)
            self.sizes[url] = size
            self.is_folder[url] = is_folder

    def run(self):
        while self.urls_to_visit:
            url = self.urls_to_visit.pop(0)

            text, status_code = self.download_url(url)

            if status_code != 200:
                logging.info(f'Failed to crawl to: {url}, status code: {status_code}')
                continue

            json_text = json.loads(text)
            if json_text.get("folder"):
                logging.info(f'Crawling: {url}')

                for child in json_text.get("children"):
                    new_url = url + "/" + child.get("name")
                    self.add_url_to_visit(new_url, child.get('size'), child.get('folder'))
            else:
                logging.info(f'Downloading file at: {url}')
                path = f'{self.download_url_path}?repoKey={json_text["repo"]}&path={json_text["path"].replace("/", "%252F")}'
                print("ATENTIE!")
                print(path)
                download_loc = self.download_folder + "/" + json_text["path"]

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

                    res = self.session.get(path, verify=self.verify, cookies=self.cookies)
                    self.cookies = res.cookies

                    open(download_loc, 'wb').write(res.content)
                    logging.info(f'Finished downloading: {path}')
                    self.downloaded_links.append(url)
                else:
                    logging.info(f'File already exists: {path}')


if __name__ == '__main__':
    config = json.load(open('config.json'))

    if config["logging"]:
        logfile = f"debug-{datetime.today().strftime('%Y-%m-%d-%H%M%S')}.log"
        file = Path(logfile)
        file.touch(exist_ok=True)

        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(message)s',
            level=logging.INFO,
            handlers=[
                logging.FileHandler(logfile),
                logging.StreamHandler()])


    Crawler(urls=config["urls"],
            accepted_domains=config["accepted_domains"],
            download_folder=config["download_folder"],
            verify=False,
            username=config["username"],
            password=config["password"],
            login=True,
            login_url=config["login_url"],
            download_url_path=config["download_url"]).run()

