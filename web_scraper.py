from __future__ import division
import requests
from bs4 import BeautifulSoup
import re
from time import sleep
import pickle
import random


# Find 300 top grossing movies per year for 2004 - 2014
url_base = 'http://www.imdb.com/search/title?at=0&sort=boxoffice_gross_us&start=' 
url_feat = '&title_type=feature&year='

titles = {}
counts = [i for i in range(1,300,50)]
years = [i for i in range(2000, 2015)]


def get_titles(year):
    '''
    Gets 300 top-grossing film titles and their corresponding link on IMDB
    for year provided as argument
    '''
    for count in counts:
        start = count
        url = url_base + str(start) + url_feat + str(year)
        print "Getting titles " + str(start) + " for " + str(year)
        response = requests.get(url)
        page = response.text
        soup = BeautifulSoup(page)

        lst = soup.find_all(attrs={"href" : re.compile("/title/")})
        for link in lst:
            if len(link.text) > 0 and link.text != 'X':
                titles[link.attrs['href']] = link.text

# iterate through years and get titles for each year
for year in years:
    get_titles(year)

print 'Scraped %d titles' % len(titles.items())


# Create empty dictionary for collecting data for each film
movies_dct = {}

def add_movie_to_data(soup2, href):
    '''
    Hits each film page and scrapes selected data, adding to dictionary, movies_dct.
    Arguments: soup for film page, href for film page.
    '''
    # find attributes to save to dictionary
    title = soup2.find(itemprop = "name").text
    metacritic = str(soup2.find(attrs={"href" : re.compile("criticreviews?")}).text.strip())
    info = soup2.find(class_ = 'infobar')
    meta = info.find_all('meta')
    
    genres = []
    genres2 = info.find_all(itemprop='genre')
    for genre in genres2:
        genres.append(str(genre.text))
    duration = info.find(itemprop='duration').attrs['datetime']
    opening = soup2.find(text=re.compile("Opening Weekend:")).next.strip()
    studio = soup2.find(attrs={"href" : re.compile("/company/")}).text.strip()
    
    try:
        rating = info.find(itemprop='contentRating')['content']
    except TypeError:
        rating = 'UNKNOWN'
    
    try:
        release = info.find(itemprop='datePublished')['content']
    except TypeError:
        release = 0
    
    # get correct gross
    url_bus = url_page + 'business?ref_=tt_dt_bus'
    response3 = requests.get(url_bus)
    page3 = response3.text
    soup3 = BeautifulSoup(page3)
    
    try:
        gross = soup3.find(text=re.compile("Gross")).next.strip().split()[0]
    except AttributeError:
        gross = soup2.find(text=re.compile("Gross:")).next.strip()
    
    # add items to dictionary
    movies_dct[href] = {'title': title,
                         'metacritic': metacritic,
                         'rating': rating,
                         'genres': genres,
                         'release': release,
                         'duration': duration,
                         'studio': studio,
                         'gross': gross,
                         'opening': opening,
                        }
    # saves partial scrape to pickle file in case of connection interuption
    if len(movies_dct.items()) % 50 == 0:
        with open('my_data_partial.pkl', 'w') as picklefile:
            pickle.dump(movies_dct.items(), picklefile)
            print "dumped " + str(len(movies_dct.items()))
    return movies_dct


# iterates through titles in dictionary, `titles`, creates soup and calls
# function to add movie to dictionary, `movies_dct`
for href, title in titles.items():
    url_page = 'http://www.imdb.com/' + href

    try:
        response2 = requests.get(url_page)
        page2 = response2.text
        soup2 = BeautifulSoup(page2)
        add_movie_to_data(soup2, href)
        sleep(0.4)

    except AttributeError:
        print title + ' did not load'
        sleep(0.2)


# saves completed scrape to pickle file
with open('my_data_2000_2014.pkl', 'w') as picklefile:
    pickle.dump(movies_dct.items(), picklefile)



### DIDN"T SCRAPE ENOUGH THE FIRST TIME?
# Go back to IMDB and SCRAPE ALL THE THINGS!

# open previously scraped and pickled data
with open("my_data_2000_2014.pkl", 'r') as picklefile: 
    raw_data1 = pickle.load(picklefile)

data = {item[0] : item[1]
        for item in raw_data1}


def get_more_data(soup, href):
    '''
    Hits each film page and scrapes selected data, adding to dictionary, `data`.
    Arguments: soup for film page, href for film page.
    '''
    user_score = float(str(soup.find(class_ = 'star-box-giga-star').text))
    num_users = str(soup.find(itemprop = 'ratingCount').text)
    num_reviews = str(soup.find(itemprop = 'reviewCount').text).split()[0]
    director = soup.find(text=re.compile("Director:")).next.next.text
    lead = soup.find(text=re.compile("Stars:")).next.next.text
    country = soup.find(text=re.compile("Country:")).next.next.text
    language = soup.find(text=re.compile("Language:")).next.next.text
    
    data[href]['user_score'] = user_score
    data[href]['num_users'] = num_users
    data[href]['num_reviews'] = num_reviews
    data[href]['director'] = director
    data[href]['lead'] = lead
    data[href]['country'] = country
    data[href]['language'] = language


def load_page(href):
    '''
    Loads film page and converts to soup for scraping.
    Calls get_more_data function for each page.
    Arguments: href for film page
    '''
    url_page = 'http://www.imdb.com/' + href

    try:
        response = requests.get(url_page)
        page = response.text
        soup = BeautifulSoup(page)
        get_more_data(soup, href)
        sleep(0.4)

    except AttributeError:
        print href + ' did not load'
        sleep(0.2)


# iterates through hrefs in data and adds more fields
counter = 0
for href in data.keys():
    load_page(href)
    counter += 1
    if counter % 50 == 0:
        with open('my_data_partial2.pkl', 'w') as picklefile:
            pickle.dump(data, picklefile)
            print "dumped " + str(counter)


with open('my_data_2000_2014_02.pkl', 'w') as picklefile:
    pickle.dump(data, picklefile)

