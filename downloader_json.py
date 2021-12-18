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


def get_domains(accepted_domains, urls):
    """
    If accepted_domains is empty, create it from the domains of all the urls given as input
    """
    if len(accepted_domains) == 0:
        for url in urls:
            domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(url))
            accepted_domains.append(domain)
    return accepted_domains


class Crawler:
    def __init__(self, urls=[], accepted_domains=[], download_folder='download/', verify=True, username='',password='', login=True, login_url='', download_url_path=''):
        """Constructs all necessary atributes, and generates the environment for the crawler

        Parameters
        ----------
            urls : list(str)
                list of the urls to be crawler
            accepted_domains: list(str)
                list of the accepted domains the crawler is alowed to go into
            download_folder: str
                folder in which the crawler will download the files
            verify: bool
                SSL Cert Verification used by the requests library
            login: bool
                if login is true the crawler will try to authentificate with username and password at the login_url
        """
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

        # header used for HEAD requests
        self.head_headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'close'
        }

        if login:
            # header and body of the login POST request
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
        """Used to retrieve the html of the url param

        Parameters
        ---------
        url: str
            url for request

        Returns
        ---------
        res.text: str
            The html text
        res.status_code: integer
            The status_code returned by the response
        """
        res = self.session.get(url, verify=self.verify, cookies=self.cookies)
        self.cookies = res.cookies

        return res.text, res.status_code

    def add_url_to_visit(self, url, size, is_folder):
        """When a url is to be added it verifies if it's domain is in the list of acceptable domains
        and if it hasn't been visited, or hasn't been added to the urls_to_visit list

        Parameters
        ----------
        url: str
            url for request
        size: str
            if the url contains a file, this parameter holds its size
        is_folder: bool
            if the url is a json page or not
        """
        domain ='{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(url))

        if url not in self.visited_urls and url not in self.urls_to_visit and domain in self.accepted_domains:
            self.urls_to_visit.append(url)
            self.sizes[url] = size
            self.is_folder[url] = is_folder

    def run(self):
        """ Main function of the crawler that contains most of the logic necessary for the crawl
                """

        # a breadth first search in the queue of urls, starting with the urls given in the constructor of the class.
        while self.urls_to_visit:
            # get the next url to explore
            url = self.urls_to_visit.pop(0)
            # retrieve the body and the status code of the url
            text, status_code = self.download_url(url)

            if status_code != 200:
                logging.warning(f'Failed to crawl to: {url}, status code: {status_code}')
                continue

            # load json
            json_text = json.loads(text)
            if json_text.get("folder"):
                logging.info(f'Crawling: {url}')

                # loop through all the children of the folder and add them to the url_to_visit list
                for child in json_text.get("children"):
                    new_url = url + "/" + child.get("name")
                    self.add_url_to_visit(new_url, child.get('size'), child.get('folder'))
            else:
                logging.info(f'Downloading file at: {url}')
                # construct the download path and the download folder
                path = f'{self.download_url_path}?repoKey={json_text["repo"]}&path={json_text["path"].replace("/", "%252F")}'
                download_loc = self.download_folder + "/" + json_text["path"]

                try:
                    # try retrieving the local size of the file
                    f_size = str(os.path.getsize(download_loc))
                except Exception:
                    # and if it doesn't exist we use a default value
                    f_size = 'Doesn\'t exist'
                finally:
                    logging.info(f'Local size: {f_size}; Server size: {self.sizes.get(url)}')

                # check if the file doesn't exist or has different size from the one found on the site
                if not os.path.isfile(download_loc) or f_size != self.sizes.get(url):
                    # create folder for the download location
                    if not os.path.exists(dir_path := os.path.dirname(download_loc)):
                        os.makedirs(dir_path)
                        os.chmod(dir_path, 666)

                    # download from the url and write it locally
                    res = self.session.get(path, verify=self.verify, cookies=self.cookies)
                    self.cookies = res.cookies

                    open(download_loc, 'wb').write(res.content)
                    logging.info(f'Finished downloading: {path}')
                    # add to the already downloaded list
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
            login=config["login"],
            login_url=config["login_url"],
            download_url_path=config["download_url"]).run()

