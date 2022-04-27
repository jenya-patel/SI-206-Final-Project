#Our API Key

from doctest import master
from multiprocessing.spawn import spawn_main
from unicodedata import name
from xml.sax import parseString
from bs4 import BeautifulSoup
from numpy import inner, sort
import requests
import re
import os
import csv
import sqlite3
import os
import matplotlib.pyplot as plt 
import matplotlib.ticker as plticker
import numpy as np
import unittest
import json
from numpy.core.fromnumeric import size
import requests
import pandas as pd


## Team Name: The Yelpers 
## Team Members: Tarush Nandrajog, Jenya Patel

def YelpAPI(city):
# Setting up the API Key
    API_Key = "fmWPV0QXnVOcGeraNFnd0S1w4DBVTbakC6PfqMY-Rw759x9LlDPT3Fa35EzguQefCd894hqHE1THpPEcqMYZIUpxK0h1lRo_h2VrqSOrEnocPRJilY64gZltGCFbYnYx"
    ENDPOINT = "https://api.yelp.com/v3/businesses/search"
    HEADERS = {'Authorization': "bearer %s" % API_Key} 

    PARAMETERS1 = {'location': city,
    'limit': 25,
    'radius': '1000',
    'categories': 'education'
    }

    #Make a request to get the Yelp API and covert data into dictionary format
    schools = requests.get(url = ENDPOINT, params = PARAMETERS1, headers = HEADERS)
    business_data = schools.json()
    return business_data


# Create project database
def setUpDatabase(db_name):
    # Takes in database name (string) as input. Returns database cursor and connection as outputs.
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn


def setUpYelpTable(business_data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Yelp (School UNIQUE PRIMARY KEY, Review_Count INTEGER, Rating NUMERIC, State_id INTEGER, City_id INTEGER, Zip_Code INTEGER)")
    # business_names = business_data['businesses']['name']
    for i in range(len(business_data['businesses'])):
        business_names = business_data['businesses'][i]['name']
        review_counts = business_data['businesses'][i]['review_count']
        ratings = business_data['businesses'][i]['rating']
        business_states = business_data['businesses'][i]['location']['state']
        # print(business_states)
        cur.execute("SELECT id FROM States WHERE State = ?", (business_states,))
        state_id = cur.fetchone()
        
        business_cities = business_data['businesses'][i]['location']['city']
        # if business_cities == 'None':
        #     business_cities = "no_city"
        # try
        cur.execute("SELECT id FROM Census WHERE City = ?", (business_cities,))
        city_id = cur.fetchone()
        # print(business_cities)
        
        zip_codes = business_data['businesses'][i]['location']['zip_code']

        # print(city_id)
        # print(city_id[0])
        cur.execute('INSERT OR IGNORE INTO Yelp (School, Review_Count, Rating, State_id, City_id, Zip_Code) VALUES (?, ?, ?, ?, ?, ?)', (business_names, review_counts, ratings, int(state_id[0]), int(city_id[0]), zip_codes))
    conn.commit()


def YelpStatesTable(business_data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS States (id INTEGER IDENTITY(1,1), State TEXT UNIQUE PRIMARY KEY)")
    cur.execute("SELECT COUNT(*) FROM States")
    id = cur.fetchall()
    # print(id)
    for i in range(len(business_data['businesses'])):
        state = business_data['businesses'][i]['location']['state']
        cur.execute('INSERT OR IGNORE INTO States (id, State) VALUES (?, ?)', (id[0][0], state))
    conn.commit()


def extract_income_data(income_data):
    med_income = []
    soup = BeautifulSoup(income_data, 'html.parser')
    incomes = soup.find_all('div', class_ = 'qf-graph-bar')
    for income in incomes:
        med_income.append(income.text.strip())
        # city_list.append(city['data-fullname'].strip(", Illinois"))
    return med_income  

def get_income_data():
    
    income_list = []

    income_list.extend(extract_income_data(requests.get("https://www-census-gov.proxy.lib.umich.edu/quickfacts/geo/chart/phoenixcityarizona,detroitcitymichigan,michigancitycityindiana,MI/INC110220").text))
    income_list.extend(extract_income_data(requests.get("https://www-census-gov.proxy.lib.umich.edu/quickfacts/geo/chart/detroitcitymichigan,michigancitycityindiana,MI/INC110220").text))
    income_list.extend(extract_income_data(requests.get("https://www-census-gov.proxy.lib.umich.edu/quickfacts/geo/chart/californiacitycitycalifornia,CA/INC110220").text))
    income_list.extend(extract_income_data(requests.get("https://www-census-gov.proxy.lib.umich.edu/quickfacts/geo/chart/fortlauderdalecityflorida,detroitcitymichigan,michigancitycityindiana,MI/INC110220").text))
    income_list.extend(extract_income_data(requests.get("https://www-census-gov.proxy.lib.umich.edu/quickfacts/geo/chart/clevelandheightscityohio,michigancitycityindiana,losangelescitycalifornia,detroitcitymichigan,fortlauderdalecityflorida/INC110220").text))

    return income_list

def extract_city_data(state_data):
    names_city = []
    soup = BeautifulSoup(state_data, 'html.parser')
    cities = soup.find_all('a', class_ = 'addgeo icon-plus')
    for city in cities:
        city = city['data-fullname'].split(" ")
        city = ' '.join(city[0: -2])
        # city_list.append(city['data-fullname'].strip(", Illinois"))
        names_city.append(city)
    return names_city

def get_city_data():
    city_list = []
    city_list.extend(extract_city_data(requests.get("https://www-census-gov.proxy.lib.umich.edu/quickfacts/geo/chart/phoenixcityarizona,detroitcitymichigan,michigancitycityindiana,MI/INC110220").text))
    city_list.extend(extract_city_data(requests.get("https://www-census-gov.proxy.lib.umich.edu/quickfacts/geo/chart/detroitcitymichigan,michigancitycityindiana,MI/INC110220").text))
    city_list.extend(extract_city_data(requests.get("https://www-census-gov.proxy.lib.umich.edu/quickfacts/geo/chart/californiacitycitycalifornia,CA/INC110220").text))
    city_list.extend(extract_city_data(requests.get("https://www-census-gov.proxy.lib.umich.edu/quickfacts/geo/chart/fortlauderdalecityflorida,detroitcitymichigan,michigancitycityindiana,MI/INC110220").text))
    city_list.extend(extract_city_data(requests.get("https://www-census-gov.proxy.lib.umich.edu/quickfacts/geo/chart/clevelandheightscityohio,michigancitycityindiana,losangelescitycalifornia,detroitcitymichigan,fortlauderdalecityflorida/INC110220").text))

    # print(city_list)
    return city_list

def setUpCensusTable(city_list, income_list, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Census (id INTEGER UNIQUE PRIMARY KEY, City TEXT, Median_income TEXT)")
    for i in range(len(city_list)):
        cur.execute('INSERT OR IGNORE INTO Census (id, City, Median_income) VALUES (?, ?, ?)', (i, city_list[i], income_list[i]))
    conn.commit()


def calculate_avg_rating(cur, conn):
    cur.execute(
        """
        SELECT AVG(Yelp.Rating), Census.City
        FROM Yelp
        JOIN Census ON Census.id = Yelp.City_id
        GROUP BY Yelp.City_id
        """
    )
    # avg_city_rating = ('SELECT AVG(Rating) FROM Yelp GROUP BY City;')
    # cur.execute(avg_city_rating)
    # print("The average education rating for each city is:")
    ratings = cur.fetchall()
    # print(ratings)
    return ratings

def write_avg_rating_to_file(filename, avg_city_rating):
    '''Takes in filename (string) and a list of tuples from join_tables().
    Returns text file ('difference.txt') that writes the difference value of NY/National COVID-19 cases for specified 100 days.'''
    with open(filename, "w", newline="") as fileout:
        fileout.write("Average Ratings of Schools in Each City:\n")
        fileout.write("======================================================================================\n\n")
        for i in range(len(avg_city_rating)):
            fileout.write("The average rating for schools in {} was {}.\n".format(avg_city_rating[i][1], avg_city_rating[i][0]))
        fileout.close()
    

def calculate_total_reviews(cur, conn):
    cur.execute(
        """
        SELECT SUM(Yelp.Review_Count), States.State
        FROM Yelp
        JOIN States ON States.id = Yelp.State_id
        GROUP BY Yelp.State_id
        """
    )
    res = cur.fetchall()
    return res

def write_total_reviews_file(filename, total_reviews):
    with open(filename, "w", newline="") as fileout:
        fileout.write("Total Number of Reviews for Schools in Each State:\n")
        fileout.write("======================================================================================\n\n")
        for i in range(len(total_reviews)):
            fileout.write("The total number of reviews for schools in {} was {}.\n".format(total_reviews[i][1], total_reviews[i][0]))
        fileout.close()


def calc_scatter_plot(cur, conn):
    cur.execute(
        """
        SELECT AVG(Yelp.Rating), Census.Median_income, Census.City
        FROM Census
        JOIN Yelp ON Yelp.City_id = Census.id
        GROUP BY Yelp.City_id
        """
    )
    res = cur.fetchall()
    return res

def write_scatter_data_to_file(filename, scatter_data):
    with open(filename, "w", newline="") as fileout:
        fileout.write("Average Ratings of Schools vs Median Income of the City:\n")
        fileout.write("======================================================================================\n\n")
        for i in range(len(scatter_data)):
            fileout.write("The average rating for schools in {} with a median income of {} was {}.\n".format(scatter_data[i][2], scatter_data[i][1], scatter_data[i][0]))
        fileout.close()

def pie_chart_totals(cur, conn):
    # Only one state popping up
    sizes = calculate_total_reviews(cur, conn)
    # returns list of tuples with (review count, state)
    labels = []
    for size in sizes:
        labels.append(size[1])
    sizes_list = []
    for size in sizes:
        sizes_list.append(size[0])
    # print(sizes_list)
    patches, texts = plt.pie(sizes_list, startangle=140)
    plt.title('Number of Education Reviews by State')
    plt.axis('equal')
    # labels = [f'{l}, {s:0.1f}%' for l, s in zip(labels, sizes)]
    plt.legend(patches, labels, loc='upper right', bbox_to_anchor=(-0.1, 1.), fontsize=8)
    plt.tight_layout()
    plt.show()
    

def bar_chart_avg_rating(cur, conn):
    avg_city_rating_tuples = calculate_avg_rating(cur, conn)
    avg_city_rating_list = []
    for i in avg_city_rating_tuples:
        avg_city_rating_list.append(i[0])
    
    x_axis_labels = []
    for city in avg_city_rating_tuples:
        x_axis_labels.append(city[1])
    
    plt.bar(x_axis_labels, avg_city_rating_list, color = 'Blue')
    plt.xticks(fontsize=6)
    plt.xticks(rotation=45)
    plt.xlabel('City')
    plt.ylabel('Average Rating of Education Services')
    plt.title('Average Rating of Education Services in Each City')

    plt.show()
    

def scatter_plot_rating_income(cur, conn):
    scatter_calc = calc_scatter_plot(cur, conn)
    # print(scatter_calc)
    
    x_axis_labels = []
    y_axis_labels = []
    for i in scatter_calc:
        x_axis_labels.append(i[1])
        y_axis_labels.append(i[0])

    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.scatter(x_axis_labels, y_axis_labels, linestyle='-', marker='*')
    loc = plticker.MultipleLocator(500000)
    ax.xaxis.set_major_locator(loc)
    plt.ylabel('School Yelp Rating')
    plt.xlabel('Median Income of School City')
    plt.title("Median Income of School Cities vs School Rating")
    plt.show()


def calculate_avg_rating_by_state(cur, conn):
    cur.execute(
        """
        SELECT AVG(Yelp.Rating), States.State
        FROM Yelp
        JOIN States ON Yelp.State_id = States.id
        GROUP BY Yelp.State_id
        """
    )
    state_ratings = cur.fetchall()
    # print(ratings)
    return state_ratings

def write_avg_rating_by_state_to_file(filename, avg_state_rating):
    with open(filename, "w", newline="") as fileout:
        fileout.write("Average Ratings of Schools in Each State:\n")
        fileout.write("======================================================================================\n\n")
        for i in range(len(avg_state_rating)):
            fileout.write("The average rating for schools in {} was {}.\n".format(avg_state_rating[i][1], avg_state_rating[i][0]))
        fileout.close()

def horizontal_bar_chart_state_avg(cur, conn):
    avg_state_rating_tuples = calculate_avg_rating_by_state(cur, conn)
    avg_state_rating_list = []
    for i in avg_state_rating_tuples:
        avg_state_rating_list.append(i[0])
    
    x_axis_labels = []
    for city in avg_state_rating_tuples:
        x_axis_labels.append(city[1])
    
    plt.barh(x_axis_labels, avg_state_rating_list, color = 'Red')
    plt.xticks(fontsize=6)
    plt.xticks(rotation=45)
    plt.xlabel('Average Rating of Education Services')
    plt.ylabel('States')
    plt.title('Average Rating of Education Services in Each State')

    plt.show()

def main():
    cur, conn = setUpDatabase('total_data.db')
    
    city_list = get_city_data()
    income_list = get_income_data()
    setUpCensusTable(city_list, income_list, cur, conn)
    
    city_2 = ['Dayton', 'Tucson', 'Kalamazoo', 'Boca Raton', 'Miami', 'Akron', 'Grand Rapids', 'Sacramento', 'Gainesville', 'Santa Clara']
    # city_2 = ['Gainesville']
    try:
        cur.execute("SELECT COUNT(id) FROM States")
        i = cur.fetchone()
        city = city_2[i[0]]
    except:
        i = 0
        city = city_2[i]
    business_data = YelpAPI(city)
    YelpStatesTable(business_data, cur, conn)
    setUpYelpTable(business_data, cur, conn)
    # YelpCitiesTable(business_data, cur, conn)

    
    # calculate_total_reviews(cur, conn)
    total_reviews = calculate_total_reviews(cur,conn)
    write_total_reviews_file('total_reviews.txt', total_reviews)
   
    avg_city_rating = calculate_avg_rating(cur, conn)
    write_avg_rating_to_file('avg_ratings.txt', avg_city_rating)
    
    pie_chart_totals(cur, conn)
    bar_chart_avg_rating(cur, conn)

    scatter_data = calc_scatter_plot(cur, conn)
    write_scatter_data_to_file('income_vs_ratins.txt', scatter_data)
    scatter_plot_rating_income(cur, conn)

    avg_state_rating = calculate_avg_rating_by_state(cur, conn)
    write_avg_rating_by_state_to_file('avg_state_ratings.txt', avg_state_rating)
    horizontal_bar_chart_state_avg(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()