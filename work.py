def get_wikipedia_page(url):
    import requests

    print("getting wikipedia page..", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        return response.text

    except requests.RequestException as e:
        print(f"An error occured:{e}")

def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')

    table = soup.find_all("table", {"class": "wikitable sortable"})[0]
    table_rows = table.find_all('tr')

    return table_rows





def extract_wikipedia_data(url):

    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)
    print(rows)

extract_wikipedia_data('https://en.wikipedia.org/wiki/List_of_U.S._stadiums_by_capacity')