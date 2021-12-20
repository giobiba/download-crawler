import logging
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import os
import json
from datetime import datetime
from pathlib import Path
import requests


def remove_control(line):
    return ''.join(c for c in line if ord(c) >= 32)


def is_absolute(url):
    return bool(urlparse(url).netloc)


def get_linked_urls(url, html):
    """Parse an html page and yield the paths of other urls with their given size (if the url contains a file)
    """
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all('a'):
        path = link.get('href')

        if path == "../":
            continue

        if not is_absolute(path):
            path = urljoin(url, path)

        size = remove_control(str(link.nextSibling)).split(' ')[-1]
        yield path, size


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
    def __init__(self, urls=[], accepted_domains=[], download_folder='download/', verify=True, username='',password='', login=False, login_url=''):
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
        self.accepted_domains = get_domains(accepted_domains, urls)
        self.urls_to_visit = urls
        # contains the sizes of the files that are to be downloaded for comparison with already existing local files
        self.sizes = dict()
        self.download_folder = download_folder
        self.verify = verify
        self.session = requests.session()
        self.cookies = dict()

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
        """
        res = self.session.get(url, verify=self.verify, cookies=self.cookies)
        # update cookies
        self.cookies = res.cookies

        return res.text

    def add_url_to_visit(self, url, size):
        """When a url is to be added it verifies if it's domain is in the list of acceptable domains
        and if it hasn't been visited, or hasn't been added to the urls_to_visit list

        Parameters
        ----------
        url: str
            url for request
        size: str
            if the url contains a file, this parameter holds its size
        """
        domain ='{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(url))

        if url not in self.visited_urls and url not in self.urls_to_visit and domain in self.accepted_domains:
            self.urls_to_visit.append(url)
            self.sizes[url] = size

    def crawl(self, url):
        """Download the html from the url, and for all the links existent in the page, add them if they are valid
        """
        html = self.download_url(url)

        for url, size in get_linked_urls(url, html):
            self.add_url_to_visit(url, size)

    def run(self):
        """ Main function of the crawler that contains most of the logic necessary for the crawl
        """

        # a breadth first search in the queue of urls, starting with the urls given in the constructor of the class.
        while self.urls_to_visit:
            # get the next url to visit
            url = self.urls_to_visit.pop(0)

            # retrieve the header of the url
            r = requests.head(url, verify=self.verify, cookies=self.cookies, headers=self.head_headers)
            self.cookies = r.cookies

            if r.status_code != 200:
                logging.info(f'Failed to crawl to: {url}, status code: {r.status_code}')
                continue

            # if the url request gives us an html means that we can crawl through it to get other links
            if "text/html" in r.headers["content-type"]:
                logging.info(f'Crawling: {url}')
                try:
                    self.crawl(url)
                except Exception as e:
                    logging.exception(f'Failed to crawl: {url}; with exception: {e}')
                finally:
                    # mark as visited
                    self.visited_urls.append(url)
            # otherwise this url is downloaded locally
            else:
                # skip if the url doesn't match the given regex pattern
                if self.re_prog.pattern != "" and not bool(self.re_prog.fullmatch(url)):
                    logging.info(f'Skipped url: {url}')
                    continue

                logging.info(f'Downloading: {url}')
                parsed_url = urlparse(url)

                # determine where to download the file
                download_loc = self.download_folder + parsed_url.path

                logging.info(f'Download location for {url} is {download_loc}')

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
                    res = self.session.get(url, verify=self.verify, cookies=self.cookies)
                    self.cookies = res.cookies

                    with open(download_loc, 'wb') as f:
                        for chunk in res.iter_content(chunk_size=8096):
                            if chunk:
                                f.write(chunk)
                    logging.info(f'Finished downloading: {url}')
                else:
                    logging.info(f'File already exists: {url}')
            self.visited_urls.append(url)

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
            verify=config["verify"],
            regex=config["regex"]).run()



