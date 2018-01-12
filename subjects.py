# -*- coding: utf-8 -*-
from scrapy import Spider
from scrapy.http import Request
import os
import glob
import csv
import sqlite3


class SubjectsSpider(Spider):
    name = 'subjects'
    # allowed_domains = ['https://www.class-central.com']
    start_urls = ['https://www.class-central.com/subjects/']

    # USAGE: scrapy crawl subjects -a subject="Engineering"
    def __init__(self, subject=None):
        self.subject = subject

    def parse(self, response):

        if self.subject:
            print('\n')
            self.logger.info('SCRAPING SUBCATEGORY: ' + self.subject + '\n')
            subject = response.xpath(
                '//*[contains(@title, "' + self.subject + '")]/@href').extract_first()
            absURL = response.urljoin(subject)
            # print('\n' + absURL + '\n')
            yield Request(absURL, callback=self.parse_subject)
        else:
            print('\n')
            self.logger.info('SCRAPING ALL PAGES \n')
            subjects = response.xpath('//*[@class="category-header"]/a/@href').extract()
            for subject in subjects:
                absURL = response.urljoin(subject)
                # print(absURL + '\n')
                yield Request(absURL, callback=self.parse_subject)

    def parse_subject(self, response):
        subject_name = response.xpath('//title/text()').extract_first()
        subject_name = subject_name.split(' | ')
        subject_name = subject_name[0]

        courses = response.xpath('//*[@class="course-name"]')
        for course in courses:
            course_title = course.xpath('.//@title').extract_first()
            rel_course_href = course.xpath('.//@href').extract_first()
            abs_course_href = response.urljoin(rel_course_href)

            yield{
                'subject': subject_name,
                'title': course_title,
                'URL': abs_course_href,
            }

        rel_next_url = response.xpath('//*[@rel="next"]/@href').extract_first()
        abs_next_url = response.urljoin(rel_next_url)
        yield Request(abs_next_url, callback=self.parse_subject)

    def close(response, reason):
        path = os.getcwd()
        print('\n' + ' Path: ' + path + '\n')

        try:
            csv_file = max(glob.iglob('*.csv'), key=os.path.getctime)
            os.rename(csv_file, 'foo.csv')
        except Exception:
            print('\n ERROR: please define file output! \n')

        conn = sqlite3.connect('subjects.db')
        c = conn.cursor()

        def drop_table():
            c.execute("DROP TABLE IF EXISTS subjectsTable")

        def mk_table():
            c.execute(
                """CREATE TABLE IF NOT EXISTS subjectsTable(subject TEXT, title TEXT, URL TEXT)""")

        drop_table()
        mk_table()

        with open('foo.csv') as f:
            readcsv = csv.reader(f, delimiter=',')
            # titles = []

            for row in readcsv:
                # print(row)
                # titleCol = row[2]
                # priceCol = row[1]
                # titles.append(titleCol)
                # price.append(priceCol)
                c.execute(
                    "INSERT INTO subjectsTable(subject, title, URL) VALUES(?, ?, ?)", row)

        conn.commit()
        conn.close()
