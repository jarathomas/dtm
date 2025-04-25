from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import pandas as pd
import re
import os

def get_headers():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://read.dukeupress.edu/demography/issue/61/1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Sec-GPC': '1',
        'Priority': 'u=0, i',
    }

    return headers

def get_cookies():
    cookies = {
        'DUP_SessionId': 'cu1nan4fwkkcqfqgzvixa52h',
        'Duke_University_PressMachineID': '638568086840727696',
        'hum_dup_visitor': '021f771f-87cf-4050-aaaa-08deb12130ee',
        'GDPR_26_.dukeupress.edu': 'true',
    }

    return cookies

def get_all_issues(headers, cookies):
    all_issues = []

    url = 'https://read.dukeupress.edu/demography/list-of-years'
    response = requests.get(url, headers=headers, cookies=cookies)
    
    if response.status_code != 200:
        print(f"Failed to fetch issues, status code: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    try:
        years = [a['href'] for a in soup.find(class_='page-column--center').find_all('a') if 'href' in a.attrs]
    except AttributeError:
        print("Error parsing the years from the page.")
        return []

    for year in tqdm(years):
        response = requests.get(year, headers=headers, cookies=cookies)
        if response.status_code != 200:
            print(f"Failed to fetch year {year}, status code: {response.status_code}")
            continue
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            issues = [a['href'] for a in soup.find_all(class_='browse-by-year') if 'href' in a.attrs]
            all_issues.extend(issues)
        except AttributeError:
            print(f"Error parsing issues for year {year}.")
            continue

    return all_issues

def scrape_issues(all_issues, headers, cookies):
    all_attrs = []
    for issue in tqdm(all_issues):
        response = requests.get('https://read.dukeupress.edu' + issue, headers=headers, cookies=cookies)
        if response.status_code != 200:
            print(f"Failed to fetch issue {issue}, status code: {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            all_articles = [a['href'] for a in soup.find_all(class_='item-title') if 'href' in a.attrs]
        except AttributeError:
            print(f"Error parsing articles for issue {issue}.")
            continue

        for article in all_articles:
            attrs = {}
            response = requests.get('https://read.dukeupress.edu' + article, headers=headers, cookies=cookies)
            if response.status_code != 200:
                print(f"Failed to fetch article {article}, status code: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            try:
                attrs['Item type'] = soup.find(class_='article-client_type').text.strip()
                attrs['Issue Section'] = soup.find(class_='article-metadata-tocSections-title').find_next().text.strip() if soup.find(class_='article-metadata-tocSections-title') else ''
                attrs['Authors'] = '; '.join([a.text.strip() for a in soup.find_all(class_='al-author-name')])
                attrs['Title'] = soup.find(class_='wi-article-title').text.strip()

                text = soup.find(class_='ww-citation-primary').text
                pattern = r"^([\w\s]+) \((\d{4})\) (\d+) \(([\w\s\d]+)\): ([\w\d–]+)\."
                match = re.match(pattern, text)
                if match:
                    journal, year, volume, issue_, pages = match.groups()
                    attrs['Full journal'] = journal
                    attrs['Year'] = year
                    attrs['Volume'] = volume
                    attrs['Issue'] = issue_
                    attrs['Pages'] = pages

                attrs['Publisher'] = 'read.dukeupress.edu'
                attrs['Date published'] = soup.find(class_='article-date').text
                attrs['Link'] = soup.find(class_='citation-doi').find('a')['href']
                attrs['DOI'] = attrs['Link'].replace('https://doi.org/', '')
                attrs['Resumen'] = soup.find('h2', string='Resumen').find_next().text.strip() if soup.find('h2', string='Resumen') else ''
                attrs['Abstract'] = soup.find('h2', string='Abstract').find_next().text.strip() if soup.find('h2', string='Abstract') else ''
                attrs['Abstract'] += soup.find('h2', string='Summary').find_next().text.strip() if soup.find('h2', string='Summary') else ''
                attrs['Keywords'] = '; '.join([a.text.strip() for a in soup.find_all(class_='kwd-main')])
                attrs['Affiliation'] = soup.find(class_='aff').text if soup.find(class_='aff') else ''
                attrs['Page Link'] = 'https://read.dukeupress.edu' + article

                all_attrs.append(attrs)
            except AttributeError as e:
                print(f"Error parsing article {article}: {e}")

    return all_attrs

def main():
    path = 'C:/Users/choi.1620/OneDrive - The Ohio State University/2024 Summer OSU/RA_Demography/ab.xlsx'
    new_path = 'new_scrape.xlsx'
    df = pd.read_excel(path)
    headers = get_headers()
    cookies = get_cookies()
    all_issues = get_all_issues(headers, cookies)
    if not all_issues:
        print("No issues found. Exiting.")
        return
    all_articles = scrape_issues(all_issues, headers, cookies)
    df_new = pd.DataFrame(all_articles)
    df_new = pd.concat([df, df_new]).drop_duplicates()
    df_new.to_excel(new_path, index=False)

if __name__ == "__main__":
    main()