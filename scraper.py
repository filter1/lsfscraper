from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from lxml.html import document_fromstring

from multiprocessing import Pool


import dataset

db = dataset.connect('sqlite:///data.db')
table = db['dates']

def get_html(url):
	req = Request(url)
	try:
		response = urlopen(req)
		return response.read()
	except HTTPError as e:
		print('The server couldn\'t fulfill the request.')
		print('Error code: ', e.code)
	except URLError as e:
		print('We failed to reach a server.')
		print('Reason: ', e.reason)

def get_links_to_courses(html):
	doc = document_fromstring(str(html).encode('utf-8'))
	return [link.get('href') for link in doc.xpath('//table//a') if link.get('href') != None]


def get_all_courses():
	base_url = 'https://lsf.ovgu.de/qislsf/rds?state=wsearchv&search=1&subdir=veranstaltung&veranstaltung.semester=20141&_form=display&P_anzahl=1000&P.sort=veranstaltung.dtxt&P.vx=kurz&P_start=%s'
	start = 0
	courses = []

	while (start < 2500):
		url = base_url % start
		courses += get_links_to_courses(get_html(url))
		start += 1000

	return courses

def process_course(url):
	html = get_html(url)
	doc = document_fromstring(str(html).encode('utf-8'))

	dates = [ (row.xpath('./td[2]/text()')[0], row.xpath('./td[3]/text()')[0]) for row in doc.xpath('//img[@alt="iCalendar Export"]/../../../tr[2]')]

	res = []
	for day, time in dates:
		time_clean = time.replace('\\r','').replace('\\n','').replace('\\t','').split()
		day_clean = day.replace('\\r','').replace('\\n','').replace('\\t','')

		if len(time_clean) == 3:
			res += [dict(url=url, day=day_clean, start=time_clean[0], end=time_clean[2])]

	return res

courses = get_all_courses()

pool = Pool(20)

results = pool.map(process_course, courses)

pool.close()
pool.join()

for result in results:
	for di in result:
		table.insert(di)
