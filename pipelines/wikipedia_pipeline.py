import pandas as pd
import json
from datetime import datetime
from geopy.geocoders import Nominatim

from geopy import Nominatim


def get_wikipedia_page(url):
    import requests

    print("getting wikipedia page..", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        return response.text

    except requests.RequestException as e:
        print(f"An error occurred:{e}")


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')

    table = soup.find_all("table", {"class": "wikitable sortable"})[0]
    table_rows = table.find_all('tr')

    return table_rows


def clean_data(text):
    text = str(text).strip()
    if text.find('[') != -1:
        text = text.split('[')[0]
    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {

            'rank': i,
            'stadium': clean_data(tds[1].text).replace('â€“', '-'),
            'capacity': clean_data(tds[2].text),
            'city': tds[3].text,
            'state': tds[4].text,
            'year opened': tds[5].text,
            'type': tds[6].text,
            'tenant': tds[7].text,

        }

        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return "Done"


def get_lat_long(state, city):
    geolocator = Nominatim(user_agent='DE_P')
    location = geolocator.geocode(f'{city}, {state}', timeout=10)

    if location:
        return location.latitude, location.longitude

    return None


def transformed_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_wikipedia_data')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)

    stadiums_df['capacity'] = stadiums_df['capacity'].str.replace(',', '', regex=True)

    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    stadiums_df = stadiums_df[stadiums_df['stadium'] != 'Stadium 1']
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['state'], x['stadium']), axis=1)




    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['state'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "OK"


def write_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    #data.to_csv('data/' + file_name, index=False)
    data.to_csv('abfs://footballde@footballde.dfs.core.windows.net/data/' + file_name,
                storage_options={
                    'account_key': 'GY6WQEuuJ1gWigTSEceep3Gz1DIdwYKwI9deTbWfQileuUFVjvGzVxpThyNK9WM+rUVPjlZ9/7kw+ASty5fpTw=='
                }, index=False)

