import pymongo
import time

class book:
    
    def __init__(self, title, author, product_id, avg_rating, review_id, reviewed_by, rating, date, heading, review):
        self.title = title
        self.author = author
        self.product_id = product_id
        self.avg_rating = avg_rating
        self.review_id = review_id
        self.reviewed_by = reviewed_by 
        self.rating = rating
        self.date = date
        self.heading = heading 
        self.review = review
        
    def get_data(self):
        return {
                    "title" : self.title,
                    "author" : self.author,
                    "product_id" : self.product_id,
                    "avg_rating" : self.avg_rating,
                    "review_id" : self.review_id,
                    "reviewed_by" : self.reviewed_by, 
                    "rating" : self.rating,
                    "date" : self.date,
                    "heading" : self.heading, 
                    "review" : self.review
                }
            
    def save_to_db(self):
        data = self.get_data()
        try:
            client = pymongo.MongoClient('localhost', 27017)
            db = client['flipkart']
            collection = db['books']
            if self.review_id != "None":
                collection.update({'review_id': data['review_id']}, dict(data), upsert=True)
            else:
                collection.update({'product_id': data['product_id']}, dict(data), upsert=True)
        except BaseException, e:
            print 'failed ondata,', str(e)
            time.sleep(5)
            pass
        return True
    
    def __del__(self):
        print "deleting", self