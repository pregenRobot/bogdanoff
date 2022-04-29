import requests
import os



# response = requests.get(url, stream=True)
# with open('alaska.zip', "wb") as f:
    # for chunk in response.iter_content(chunk_size=512):
        # if chunk:  # filter out keep-alive new chunks
            # f.write(chunk)


with open("../../dumps/urls.txt") as urlf:
    all_urls:list[str] = list(urlf)


clean_urls = map( lambda url: url.strip(), all_urls)

download_urls = filter(lambda url: url[-8:] != "CHECKSUM", clean_urls)

current_downloads = list(os.walk("../../dumps/"))[0][2]

for url in list(download_urls)[:2]:
    file_name = url.split("/")[-1]
    if file_name not in current_downloads:
        print(url)




