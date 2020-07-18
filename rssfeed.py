import feedparser
import sqlite3
from time import time

NewsFeed = feedparser.parse("https://timesofindia.indiatimes.com/rssfeedstopstories.cms")

ignore_words = ["the","number", "of", "in", "to", "at", "has", "had", "have", "with", "over", "across", "for","so", "as", "is", "was", "then",
				"when", "where", "than", "on", "a", "an", "and", "before", "between", "most", "from"]


#DB initialize
conn = sqlite3.connect('local.db')
c = conn.cursor()
# Create table
#c.execute('''CREATE TABLE feed
#            (title text, summary text, link text, id text, published date, topic text)''')

#c.execute('''CREATE TABLE topics
#             (topic text, topic_alias text, topic_related Integer)''')

# Save (commit) the changes
#conn.commit()

def get_topics(query_search):
	#print(f"{query_search}")
	topic_list = []
	for row in c.execute(f"SELECT topic_no from topics WHERE {query_search}"):
		topic_list.append(row[0])
	topic_list = str(topic_list)
	#print(topic_list)
	return topic_list

def process_feed():
	"""
	Get Feed from rssurl
	"""
	print('Number of RSS posts :', len(NewsFeed.entries))
	existing_feeds = []
	for row in c.execute('SELECT link FROM feed ORDER BY rowid DESC LIMIT 8'):
		existing_feeds.append(row)

	existing_feeds = [feed[0] for feed in existing_feeds]
	#print(existing_feeds)
	
	entries = NewsFeed.entries
	new_entries = 0
	for item in entries:
		#print(f'Title: {item.title} \n summary: {item.summary} \n link: {item.link} \n id: {item.id} \n published: {item.published}')
		print(item.link)
		print()
		if item.link not in existing_feeds:
			query_search = []
			if item.summary:
				summary = set(item.summary.split())
				#print(summary)
				for word in summary:
					word = word.replace("\"","")
					if word not in ignore_words:
						query_search.append(f'keys LIKE "_%{word}_%" ')
			
				query_condition = 'OR '.join(query_search)
				
				topics = get_topics(query_condition)
				
				# Insert a row of data
				temp_tuple = (item.title, item.summary, item.link, item.id, item.published, topics)
				c.execute("INSERT INTO feed (title, summary, link, id, published, topic) VALUES (?, ?, ?, ?, ?, ?)", temp_tuple)
				new_entries += 1

		#for key,value in item.items():
		#	print("{}:{}\n\n".format(key, value))
				# Save (commit) the changes
				conn.commit()
	return new_entries

def notify_user(topics, id):
	"""
	Get the users and notify them"
	"""
	print(topics)
	for topic in topics:
		for row in c.execute(f'SELECT email FROM users WHERE ","{topic}"," IN topics'):
			print(row[0])
	

def notify_admin(id):
	"""
	Notify admin about missing optic news
	"""
	for row in c.execute(f'SELECT summary FROM feed WHERE id = {id}'):
		print(row[0])
	
def notify_new_feed(new_entry_count):
	"""
	Get the last given number of data from DB and send mail to users or alert admin if no topics
	"""
	new_feeds = []
	#Get the last added rows
	for row in c.execute(f'SELECT id, topic FROM feed ORDER BY rowid DESC LIMIT {new_entry_count}'):
		if row[1]:
			notify_user(row[1], row[0])
		else:
			notify_admin(row[0])
		
	
	

if __name__ == "__main__":
	
	new_entries = process_feed()
	notify_new_feed(new_entries)
	
# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()
	
print(f"new_entries: {new_entries}")
with open("log", "a+") as log:
	log.write(f"{time()}: {new_entries}\n")