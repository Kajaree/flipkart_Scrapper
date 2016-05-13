import gc
import sys
import time
import requests
import pymongo
from bs4 import BeautifulSoup
import json
from book import book
#####from HTMLParser import HTMLParser
from multiprocessing.dummy import Pool  # This is a thread-based Pool
from multiprocessing import cpu_count

def get_title(url):
    url_frag = url.split('/')
    return ' '.join(url_frag[3].split('-'))

def filter_review(review_text):
    review = ' '.join(review_text.split())
    print review
    return review.strip()

def prepare_query(data):
    data = data.lower()
    data = data.replace('.', '')
    query = '+'.join(data.split(' '))
    return query
    
def get_authors():
    file = open('authors.txt', 'r')
    lines = file.readlines()
    authors = []
    for line in lines:
        authors.append(line.strip())
    return authors
    
def get_anchor(url):
    url_frag = url.split('/p')
    url_id = url_frag[1].split('&')[0]
    try:
        book_author = url_frag[1].split('&')[3]
        book_author=' '.join(book_author.split('+'))
        book_author = book_author.split('=')[1]
    except:
        book_author = "Anonymous"
        
    try:
        product_id = url_id.split('=')[1]
    except:
        product_id = "None"
    anchor = url_frag[0]+'/product-reviews'+url_id+'&type=all'
    return anchor, product_id, book_author
    
def generate_all_url(start_url):
    urls = []
    urls.append(start_url)
    base_url = start_url.split('&')[0]
    next_page = "&rating=1,2,3,4,5&reviewers=all&type=all&sort=most_helpful&start="
    i = 1
    for i in range(1,10):
        j = i*10
        url = base_url + next_page + str(j)
        urls.append(url)
    return urls
        
   
def fetch_urls(page):
    soup = BeautifulSoup(page, "lxml")
    blocks = soup.find_all("div", class_="pu-details lastUnit")
    for block in blocks:
        links = block.find_all("div", class_="pu-title fk-font-13")
        for link in links:
            books = link.find("a", class_="fk-display-block")
            book_url = "http://www.flipkart.com" + books['href']
            try:
                book_page  = requests.get(book_url).text
                if fetch_book(book_url, book_page):
                    print "Book added to database."
            except requests.exceptions.Timeout:
                # Maybe set up for a retry, or continue in a retry loop
                pass
            except requests.exceptions.TooManyRedirects:
            # Tell the user their URL was bad and try a different one
                pass
            except requests.exceptions.RequestException as e:
            # catastrophic error. bail.
                print "No book found.. Finding next one."
                sys.exit(1)
            
            
def fetch_book(url, page):
    start_url, product_id, author = get_anchor(url)
    soup = BeautifulSoup(page, "lxml")
    i = 10
    try:
        details = soup.find("div", class_="product-details line")
        title = details.find("h1", class_="title").getText()
        avg_rating = soup.find_all("div", class_="fk-stars")['title']
    except:
        title = "None"
        avg_rating = "None"
    if title == "None":
        title = get_title(url)
    urls = generate_all_url(start_url)
    for url in urls:
        j = 0
        print url
        try:
            book_page = requests.get(url).text  
            j += 1
        except requests.exceptions.Timeout:
            # Maybe set up for a retry, or continue in a retry loop
            i = 0
        except requests.exceptions.TooManyRedirects:
            # Tell the user their URL was bad and try a different one
            i = 0
        except requests.exceptions.RequestException as e:
            # catastrophic error. bail.
            print e
            i = 0
            sys.exit(1)
        
        if i != 0:
            i, books = fetch_reviews(book_page)
            if len(books) != 0:
                for b in books:
                    new_book = book(title, author, product_id, avg_rating, b['review_id'], b['reviewed_by'], b['rating'], b['date'], b['heading'], b['review'])
                    if new_book.save_to_db():
                        print new_book.get_data()
                        gc.disable()   
                        del new_book # nothing gets deleted here
                        print "after"
                        gc.collect()
                        print gc.garbage # The GC knows the two Foos are garbage, but won't delete
                        # them because they have a __del__ method
                        print "after gc" 
            else:
                new_book = book(title, author, product_id, avg_rating, "None", "None", "None", "None", "None", "None")
                if new_book.save_to_db():
                    print new_book.get_data()
                    gc.disable()   
                    del new_book # nothing gets deleted here
                    print "after"
                    gc.collect()
                    print gc.garbage # The GC knows the two Foos are garbage, but won't delete
                    # them because they have a __del__ method
                    print "after gc" 
        elif i == 0:
            print "No more remains."
            break  
    return True


def fetch_reviews(page):
    soup = BeautifulSoup(page, "lxml")
    blocks = soup.find_all("div", class_="fclear fk-review fk-position-relative line ")
    book_details = []
    i = 0
    for block in blocks:
        review_id = block['review-id']
        rating = block.find("div", class_="fk-stars")['title']
        try:
            reviewed_by = block.find("a", class_="load-user-widget fk-underline").getText()
            review_text = block.find("span", class_="review-text").getText()
        except:
            reviewed_by = "Anonymous"
            review_text = "No reviews found"
        date = block.find("div", class_="date line fk-font-small").getText().strip()
        head = block.find("div", class_="line fk-font-normal bmargin5 dark-gray")
        heading = head.find("strong").getText().strip()   
        review = filter_review(review_text)
        i = i + 1
        print "No of Reviews: " , i
        book_detail = { 
                            "review_id" : review_id,
                            "reviewed_by" : reviewed_by, 
                            "rating" : rating,
                            "date" : date,
                            "heading" : heading, 
                            "review" : review}
        book_details.append(book_detail)
    if len(book_details) == 0:
        i = 0
        book_detail = { 
                            "review_id" : "None",
                            "reviewed_by" : "None", 
                            "rating" : "None",
                            "date" : "None",
                            "heading" : "None", 
                            "review" : "None"}
        book_details.append(book_detail)
    return i, book_details
    
    
base_url = "http://www.flipkart.com/lc/pr/pv1/spotList1/spot1/productList?sid=search.flipkart.com&filterNone=true&ajax=true&start="
author = raw_input("Enter the author: ")
print author
query = prepare_query(author)
no_bad_request = True
i = 0
while no_bad_request == True:
    i += 1
    url = base_url + str(i) + "&q=" + query 
    print url
    try:
        page  = requests.get(url).text
        fetch_urls(page)
    except requests.exceptions.Timeout:
        # Maybe set up for a retry, or continue in a retry loop
        no_bad_request = False
    except requests.exceptions.TooManyRedirects:
    # Tell the user their URL was bad and try a different one
        no_bad_request = False
    except requests.exceptions.RequestException as e:
    # catastrophic error. bail.
        print e
        no_bad_request = False
        sys.exit(1)
    except:
        print "Nothing found. Processing the nest author."
        no_bad_request = False
        sys.exit(1)
    if (i == 1) & (no_bad_request == False):
        print "No book by ", author, ", is found..."

    