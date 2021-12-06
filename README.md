# download-crawler
A python crawler that downloads all links of file from given links


## Description
This crawler was intended for a specific format where an a tag is followed by the format "<<date>> <<size>>". (with no specific date format, but a size in bytes)
This is necessary for extracting the size of the file correctly before downloading it.
If this requirement is fulfilled it will only download the files that aren't already downloaded, or modified files, thus reducing the total time.
It can resume a download if files were already downloaded at a previous point.
