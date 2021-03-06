import logging
from urllib.parse import urlparse
import os
import json
from datetime import datetime
from pathlib import Path
import requests
import re
import signal
import sys
import subprocess


def add_unique_postfix(loc, fn):
    """
    Given a filename and a location it checks if the file exists, and if it does it adds a unique postfix
    If the file doesn't exist it simply return its name
    """
    path = os.path.join(loc, fn)

    if not os.path.exists(path):
        return fn

    name, ext = os.path.splitext(fn)

    make_fn = lambda i: os.path.join(loc, '%s(%d)%s' % (name, i, ext))

    for i in range(1, sys.maxsize):
        uni_fn = make_fn(i)
        if not os.path.exists(uni_fn):
            return os.path.split(uni_fn)[1]


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
    def __init__(self, urls=None, accepted_domains=None, download_folder='download/', verify=True, username='',
                 password='', login=True, login_url='', download_url_path='', regex='', webhook_url='',
                 webhook_download_link='', files_remaining=-1):
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
        if accepted_domains is None:
            accepted_domains = []
        if urls is None:
            urls = []

        self.webhook_url = webhook_url
        self.webhook_download_link = webhook_download_link

        self.files_remaining = self.files_remaining_to_download = self.files_kept = files_remaining or -1
        self.flag = False
        self.visited_urls = []
        self.is_folder = dict()
        self.urls_to_visit = urls
        self.accepted_domains = get_domains(accepted_domains, urls)
        self.download_url_path = download_url_path
        self.download_folder = download_folder
        self.verify = verify
        self.re_prog = re.compile(regex)
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

        self.meta_path = os.path.join(self.download_folder, "meta.json")
        self.meta_data = json.loads("{}")
        self.temp_meta_data = json.loads("{}")

        # open the json metadata file, if it doesn't exist create it
        if not os.path.exists(self.meta_path):
            Path(self.meta_path).touch(666, exist_ok=True)
            os.chmod(self.meta_path, 666)
        else:
            self.meta_data = json.loads(open(self.meta_path).read() or "{}")

        def signal_handler(sig, frame):
            self.flag = True

        signal.signal(signal.SIGINT, signal_handler)

    def send_message_to_webhook(self, message):

        headers = {
            'Content-Type': 'application/json',
        }
        data = {"text": message}
        res = requests.post(self.webhook_url, headers=headers, data=json.dumps(data))

        if res.status_code == 200:
            logging.info(f'Sent "{message}" to webhook')
        else:
            logging.info(f'Failed to send "{message}" to webhook, status code: {res.status_code}')

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

    def add_url_to_visit(self, url, size, is_folder, last_modified, name, curr_path):
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
        last_modified: str
            last time the file was modified on the server
        name: str
            name of the folder/ file we want to visit
        curr_path: str
            parent url
        """
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(url))

        if url not in self.visited_urls and url not in self.urls_to_visit and domain in self.accepted_domains:
            path = curr_path + "/" + name
            self.is_folder[path] = is_folder

            if is_folder:
                self.urls_to_visit.append(url)
            else:
                # skip if the url doesn't match the given regex pattern
                if self.re_prog.pattern != "" and not bool(self.re_prog.fullmatch(name)):
                    logging.info(f'Skipped url: {url} (incompatible with the regex)')
                    return

                if self.files_remaining_to_download == 0:
                    return

                if self.files_remaining_to_download > 0:
                    self.files_remaining_to_download -= 1

                logging.info(f"Server Last Modified: {last_modified}, Server Size: {size}")
                if self.meta_data.get(path) is not None:
                    logging.info(
                        f"Client Last Modified: {self.meta_data[path].get('lastModified')}, Client Size : {self.meta_data[path].get('size')}")

                if self.meta_data.get(path) is None or (
                        last_modified != self.meta_data[path].get("lastModified") or size != self.meta_data[path].get(
                    "size")):
                    logging.info(f'Adding: {url}')
                    self.urls_to_visit.append(url)

                    if self.temp_meta_data.get(path) is None:
                        self.temp_meta_data[path] = dict()

                    if self.meta_data.get(path) is None:
                        self.temp_meta_data[path]["name"] = add_unique_postfix(self.download_folder, name)
                    else:
                        self.temp_meta_data[path]["name"] = self.meta_data[path]["name"]

                    self.temp_meta_data[path]["size"] = size
                    self.temp_meta_data[path]["lastModified"] = last_modified
                else:
                    logging.info(f"File {path} is identical.")

    def download_and_save(self, url, download_loc):
        logging.info(f"Downloading from: {url}")
        res = self.session.get(url, verify=self.verify, cookies=self.cookies)
        self.cookies = res.cookies
        logging.info(f"Finished downloading from: {url}")

        with open(download_loc, 'wb') as f:
            for chunk in res.iter_content(chunk_size=int(1e+7)):
                if chunk:
                    f.write(chunk)
        logging.info(f"Finished uploading to: {download_loc}")

    def run(self):
        """ Main function of the crawler that contains most of the logic necessary for the crawl"""
        # a breadth first search in the queue of urls, starting with the urls given in the constructor of the class.
        while self.urls_to_visit and not self.flag:
            # get the next url to explore
            url = self.urls_to_visit.pop(0)
            self.visited_urls.append(url)
            # retrieve the body and the status code of the url
            text, status_code = self.download_url(url)

            if status_code != 200:
                logging.warning(f'Failed to crawl to: {url}, status code: {status_code}')
                continue

            # load json
            json_text = json.loads(text)
            if json_text.get("folder"):
                if self.files_remaining_to_download == 0:
                    continue
                logging.info(f'Crawling: {url}')

                # loop through all the children of the folder and add them to the url_to_visit list
                for child in sorted(json_text.get("children"), key=lambda x: x.get('lastModified'), reverse=True):
                    new_url = url + "/" + child.get("name")
                    self.add_url_to_visit(new_url, child.get('size'), child.get('folder'), child.get('lastModified'),
                                          child.get('name'), json_text["path"])
            else:
                if self.files_remaining == 0:
                    self.flag = True
                    continue

                if self.files_remaining > 0:
                    self.files_remaining -= 1

                # construct the download path and the download folder
                durl = f'{self.download_url_path}?repoKey={json_text["repo"]}&path={json_text["path"].replace("/", "%252F")}'
                download_loc = os.path.join(self.download_folder,
                                            (name := self.temp_meta_data[path := json_text["path"]]["name"]))

                self.download_and_save(durl, download_loc)

                if self.webhook_url is not None and self.webhook_url != '':
                    self.send_message_to_webhook(f'File downloaded at: {self.webhook_download_link + name}')

                self.meta_data[path] = self.temp_meta_data[path].copy()
                del self.temp_meta_data[path]

        if type(self.files_kept) == int and self.files_kept > 0:
            self.clear_download_folder()

        if self.flag:
            open(self.meta_path, "w").write(json.dumps(self.meta_data))
            sys.exit(0)

    def clear_download_folder(self):
        files = ((key, value) for key, value in self.meta_data.items() if os.path.isfile(os.path.join(
            self.download_folder, value['name'])))
        files = sorted(files, key=lambda x: x[1]['lastModified'])

        for key, file_name in files[:-min(self.files_kept, len(files))]:
            try:
                os.remove(path := os.path.join(self.download_folder, file_name['name']))
                del self.meta_data[key]
                logging.info(f'Removed {path}')
            except Exception as e:
                logging.error(e)


if __name__ == '__main__':
    if len(sys.argv) >= 1:
        try:
            config = json.load(open(sys.argv[1]))
        except FileNotFoundError:
            config = json.load(open('config.json'))
    else:
        config = json.load(open('config.json'))

    if config.get("logging"):
        try:
            os.mkdir('logs')
        except:
            pass

        lista = sorted([os.path.join('logs', file) for file in os.listdir('logs') if
                        not os.path.isdir(os.path.join('logs', file))],
                       key=lambda x: os.path.getmtime(x))

        if type(config["keep-logs"]) == int and config["keep-logs"] < len(lista):
            for file in lista[:-config["keep-logs"]]:
                os.remove(file)

        logfile = f"logs/debug-{datetime.today().strftime('%Y-%m-%d-%H%M%S')}.log"
        file = Path(logfile)
        file.touch(exist_ok=True, mode=666)

        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(message)s',
            level=logging.INFO,
            handlers=[
                logging.FileHandler(logfile),
                logging.StreamHandler()])

    if config.get("download_folder") and config.get("network_user") and config.get("network_password"):
        subprocess.call(
            f'net use m: {config["download_folder"]} /user:{config["network_user"]} {config["network_password"]} /Y',
            shell=True)

    c = Crawler(urls=config["urls"],
                accepted_domains=config["accepted_domains"],
                download_folder=config["download_folder"],
                verify=config["verify"],
                username=config["username"],
                password=config["password"],
                login=config["login"],
                login_url=config["login_url"],
                download_url_path=config["download_url"],
                regex=config["regex"],
                webhook_url=config["webhook-url"],
                webhook_download_link=config["webhook-download-link"],
                files_remaining=config.get("files-count"))
    c.run()
    open(c.meta_path, "w").write(json.dumps(c.meta_data))
