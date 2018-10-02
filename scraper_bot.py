import datetime
import queue
import logging
import signal
import time
import threading
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk, VERTICAL, HORIZONTAL, N, S, E, W
import os
from tkinter import filedialog
from tkinter import *
import pandas as pd
from time import sleep
import openpyxl
import random
import requests
from bs4 import BeautifulSoup
import re
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.common.action_chains import ActionChains
import numpy as np



logger = logging.getLogger(__name__)
my_filetypes = [('csv files', '.csv')] 


""" Helper_2"""
class ScraperBot():
    def __init__(self):
        self.search_page = "https://www.google.com/shopping?hl=en"
        self.proxy_page = "https://www.vpnbook.com/webproxy"

    def make_search_query(self,index,row,sqtv):
        brand = row['BRAND']
        sku = row['SKU']
        upc = row['UPC']
        if sqtv == 'SKU & BRAND':
            if not pd.isna(sku) and not pd.isna(brand):
                q= '"{0}" and "{1}"'.format(brand,sku)
                return q
            else:
                return None
        elif sqtv == 'SKU':
            if not pd.isna(sku):
                q='"{0}"'.format(sku)
                return q
            else:
                return None
        elif sqtv == 'UPC':
            if not pd.isna(upc):
                q= '"{0}"'.format(upc)
                return q
            else:
                return None
        else:
            return None
		
    def start_driver(self):
        try:
            logger.log(logging.WARNING,' [*] starting driver...')
            dir = os.path.dirname(__file__)
            chrome_path = os.path.join(dir, 'selenium','webdriver','chromedriver.exe')
            self.driver = webdriver.Chrome(executable_path=chrome_path) 
            self.driver.maximize_window()
            sleep(1)
        except Exception as e:
            logger.log(logging.ERROR,f'Exception from: start_driver {e}')
            pass

    def close_driver(self):
        try:
            logger.log(logging.WARNING,' [*] closing driver...')
            self.driver.quit()
            logger.log(logging.WARNING,' [*] closed!')
        except Exception as e:
            logger.log(logging.ERROR,f'Exception from: close_driver {e}')
            pass

    def query_proxy_page(self,search):
        try:
            self.driver.get(self.proxy_page)
            proxy_query = self.driver.find_element_by_name('u')
            proxy_query.clear()
            proxy_query.send_keys(search)
            self.driver.find_element_by_xpath("//select[@id='webproxylocation']").send_keys(Keys.ARROW_DOWN)
            proxy_form = self.driver.find_element_by_id('webproxyform')
            proxy_form.submit()
        except Exception as e:
            logger.log(logging.ERROR,f'Exception from: query_proxy_page {e}')
            pass

    def scrape_product_html(self,link):
        try:
            logger.log(logging.WARNING,' [*] getting detailed info ...')
            final_link = 'https://www.google.com'+str(link)
            self.query_proxy_page(final_link)
            sleep(2)
            html = self.driver.page_source
            return html
        except Exception as e:
            logger.log(logging.ERROR,f'Exception from: scrape_product_html {e}')
            pass

    def query_page(self,search_query):
        try:
            logger.log(logging.WARNING,' [*] getting query page...')
            self.query_proxy_page(self.search_page)
            sleep(5)
            search_box = self.driver.find_element_by_name('q')
            search_box.clear()
            search_box.send_keys(search_query)
            form = self.driver.find_element_by_name('f')
            form.submit()
            raw_search_query = search_query.replace('"','')
            if 'and' in raw_search_query:
                search_query_split=str(raw_search_query.split('and')[1])
            else:
                search_query_split= str(raw_search_query)
            html = self.driver.page_source
            result = list()
            result.append(html)
            result.append(search_query_split)
            return result
        except Exception as e:
            logger.log(logging.ERROR,f'Exception from: query_page {e}')
            pass

    def find_link(self,html,search_query_split):
        try:
            soup = BeautifulSoup(html,"lxml")
            search_results_box = soup.find("div",id="search")
            if search_results_box != None:
                product_list = search_results_box.find("div",class_="sh-pr__product-results")
                if product_list != None:
                    product_list_collection = product_list.find_all("div",class_="sh-dlr__list-result")
                    if product_list_collection != None:
                        preferred_link = []
                        preferred_title = []
                        links = []
                        titles = []
                        for product in product_list_collection:
                            title = product.find("div",class_="eIuuYe").select("a:nth-of-type(1)")[0].text.strip()
                            link = product.find("div",class_="eIuuYe").select("a:nth-of-type(1)")[0] ['href']
                            links.append(link)
                            titles.append(title)
                            if search_query_split is list:
                                if search_query_split[1] in title:
                                    preferred_link.append(link)
                                    preferred_title.append(title)
                            elif str(search_query_split) in title:
                                preferred_link.append(link)
                                preferred_title.append(title)
                            else:
                                pass
                        if preferred_link:
                            logger.log(logging.WARNING," [*] Returning preferred link ...")
                            title_link_list = [preferred_title[0],preferred_link[0]]
                            return title_link_list
                        else: 
                            logger.log(logging.WARNING," [*] Returning first link ...")
                            title_link_list = [titles[0],links[0]]
                            return title_link_list
            return None
        except Exception as e:
            logger.log(logging.ERROR,f'Exception from: find_link {e}')
            pass

    def scrape_product_details(self,html):
        try:
            product_details = []
            soup = BeautifulSoup(html,"lxml")
            results_box = soup.find("div",id="os-sellers-content")
            if results_box:
                table = results_box.find("table",id="os-sellers-table")
                trs = table.find_all("tr",class_="os-row")
                if len(trs)>=1:
                    number_of_seller = len(trs)
                    product_details.append(number_of_seller)
                    total_price_list = []
                    shipping_price_list = []
                    for tr in trs:
                        total_price = tr.find("td",class_="os-total-col").text.strip().split('$')[1]
                        total_price_list.append(float(total_price))
                        raw_shipping_price = tr.find("td",class_="os-price-col").select("div.os-total-description")[0].text.strip()
                        if 'shipping' in raw_shipping_price:
                            raw_shipping_price_1= raw_shipping_price.split('. ')
                            for sp_list in raw_shipping_price_1:
                                if 'shipping' in sp_list:
                                    sp_price = sp_list.split()[0].replace('+$','').strip()
                                    if not 'Free' in sp_price:
                                        sp_final_price = float(sp_price)
                                        shipping_price_list.append(sp_final_price)
                    if len(total_price_list) >= 2:
                        max_value = max(total_price_list)
                        min_value = min(total_price_list)
                        product_details.append(max_value)
                        product_details.append(min_value)
                    else:
                        product_details.append(0)
                        product_details.append(0)
                    if len(total_price_list):
                        avg_value = sum(total_price_list)/len(total_price_list)
                        product_details.append(avg_value)
                    else:
                        product_details.append(0)
                    if len(shipping_price_list):
                        avg_shipping_cost = sum(shipping_price_list)/len(shipping_price_list)
                        product_details.append(avg_shipping_cost)
                    else:
                        product_details.append(0)
                    if len(total_price_list) >= 2:
                        for tr in trs:
                            total_price = tr.find("td",class_="os-total-col").text.strip().split('$')[1]
                            if float(total_price) == max_value:
                                max_seller_name = tr.find("td",class_="os-seller-name").select("span.os-seller-name-primary")[0].text.strip()
                                max_seller_link = tr.find("td",class_="os-seller-name").select("span.os-seller-name-primary")[0].select("a:nth-of-type(1)")[0]['href']
                            elif float(total_price) == min_value:
                                min_seller_name = tr.find("td",class_="os-seller-name").select("span.os-seller-name-primary")[0].text.strip()
                                min_seller_link = tr.find("td",class_="os-seller-name").select("span.os-seller-name-primary")[0].select("a:nth-of-type(1)")[0]['href']
                            else:
                                pass
                        product_details.append(max_seller_name)
                        product_details.append(max_seller_link)
                        product_details.append(min_seller_name)
                        product_details.append(min_seller_link)
                    else:
                        product_details.append('')
                        product_details.append('')
                        product_details.append('')
                        product_details.append('')
                else:
                    return product_details
            return product_details
        except Exception as e:
            logger.log(logging.ERROR,f'Exception from: scrape_product_details {e}')
            pass

    def run(self,file_name,sqtv):    
        try:
            final_dataframe = []

            search_df = pd.read_csv(file_name,sep=',',usecols=['BRAND','SKU', 'UPC','TITLE'],dtype={"BRAND":str,"SKU":str,"UPC": str,"TITLE": str},encoding='utf-8',na_values=[''] )
            for index, row in search_df.iterrows() :
                search_query = self.make_search_query(index,row,sqtv)
                if search_query != None:
                    result = self.query_page(search_query)
                    parse_link = self.find_link(result[0],result[1])
                    if parse_link != None:
                        if '/shopping/product/' in str(parse_link[1]):
                            html= self.scrape_product_html(parse_link[1])
                            if html and html != None:
                                output = self.scrape_product_details(html)
                                if len(output) >= 1:
                                    logger.log(logging.WARNING," [*] Outputting deatiled information ...")
                                    output1=output
                                    output1.insert(0,row['SKU'])
                                    output1.insert(1,row['UPC'])
                                    output1.insert(2,row['BRAND'])
                                    output1.insert(3,row['TITLE'])
                                    output1.insert(4,parse_link[0])
                                    output1.append(search_query)
                                    link = 'https://www.google.com'+str(parse_link[1])
                                    output1.append(link)
                                    final_dataframe.append(output1)
                                else:
                                    output = list()
                                    output.append(row['SKU'])#0
                                    output.append(row['UPC'])#1
                                    output.append(row['BRAND'])#2
                                    output.append(row['TITLE'])#3
                                    output.append(parse_link[0])#4 google title
                                    for i in range(5,14):
                                        output.insert(i,'')
                                    output.insert(16," [*] Don't have google shopping online stores ...")
                                    output.insert(14,search_query)
                                    if parse_link[1] != None:
                                        link = 'https://www.google.com'+str(parse_link[1])
                                        output.insert(15,link)
                                    else:
                                        output.insert(15,parse_link[1])
                                    final_dataframe.append(output)
                        else:
                            output = list()
                            output.append(row['SKU'])
                            output.append(row['UPC'])
                            output.append(row['BRAND'])
                            output.append(row['TITLE'])
                            output.append(parse_link[0])#google title
                            for i in range(5,14):
                                output.insert(i,'')
                            output.insert(16," [*] Don't have google shopping listing records, hosted outside ...")
                            output.insert(14,search_query)
                            if parse_link[1] != None:
                                link = 'https://www.google.com'+str(parse_link[1])
                                output.insert(15,link)
                            else:
                                output.insert(15,parse_link[1])
                            final_dataframe.append(output)
                    else:
                        output = list()
                        output.append(row['SKU'])
                        output.append(row['UPC'])
                        output.append(row['BRAND'])
                        output.append(row['TITLE'])
                        for i in range(4,14):
                            output.insert(i,'')
                        output.insert(16," [*] Don't have any google shopping listing search results ...")
                        output.insert(14,search_query)
                        output.insert(15,parse_link)
                        final_dataframe.append(output)
                    sleep(15)
                else:
                    logger.log(logging.WARNING,"[*] No Search Query could be generated ...")
                    pass


            def count_len(elem):
                return len(elem)

            final_dataframe.sort(reverse=False,key=count_len)    

            file_name_excel = f'scrapped_result_{sqtv}_{random.randrange(10000000000) }.xlsx'
            excel_file = pd.DataFrame(final_dataframe,columns = ['SKU','UPC','BRAND','TITLE','Google Title','Seller No.', 'Highest', 'Lowest', 'Average','Avg. Shipping Cost','HPS name','HPS link','LPS name','LPS link','Search query','Link','Error'])
            writer = pd.ExcelWriter(file_name_excel)
            excel_file.to_excel(writer,'Sheet1')
            writer.save()
            logger.log(logging.INFO,'[*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*]')
            logger.log(logging.CRITICAL,f'[*] Search for Excel File Named as "{file_name_excel}"')
            logger.log(logging.INFO,'[*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*]')
        except Exception as e:
            logger.log(logging.ERROR,f'Exception from: run {e}')
            pass


class QueueHandler(logging.Handler):

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(record)


class ConsoleUi:

    def __init__(self, frame):
        self.frame = frame
        # Create a ScrolledText wdiget
        self.scrolled_text = ScrolledText(frame, state='disabled', height=12)
        self.scrolled_text.grid(row=0, column=0, sticky=(N, S, W, E))
        self.scrolled_text.configure(font='TkFixedFont')
        self.scrolled_text.tag_config('INFO', foreground='black')
        self.scrolled_text.tag_config('DEBUG', foreground='gray')
        self.scrolled_text.tag_config('WARNING', foreground='orange')
        self.scrolled_text.tag_config('ERROR', foreground='red')
        self.scrolled_text.tag_config('CRITICAL', foreground='red', underline=1)
        # Create a logging handler using a queue
        self.log_queue = queue.Queue()
        self.queue_handler = QueueHandler(self.log_queue)
        formatter = logging.Formatter('%(asctime)s: %(message)s')
        self.queue_handler.setFormatter(formatter)
        logger.addHandler(self.queue_handler)
        # Start polling messages from the queue
        self.frame.after(100, self.poll_log_queue)

    def display(self, record):
        msg = self.queue_handler.format(record)
        self.scrolled_text.configure(state='normal')
        self.scrolled_text.insert(tk.END, msg + '\n', record.levelname)
        self.scrolled_text.configure(state='disabled')
        # Autoscroll to the bottom
        self.scrolled_text.yview(tk.END)

    def poll_log_queue(self):
        # Check every 100ms if there is a new message in the queue to display
        while True:
            try:
                record = self.log_queue.get(block=False)
            except queue.Empty:
                break
            else:
                self.display(record)
        self.frame.after(100, self.poll_log_queue)


class FormUi:

    def __init__(self, frame):
        self.frame = frame
        self.filename = ''
        # Create a combobbox to select the logging level
        values = ['SKU & BRAND', 'SKU', 'UPC']
        self.search_query_type = tk.StringVar()
        ttk.Label(self.frame, text='Search  Query Type:').grid(column=2, row=0, sticky=W)
        self.combobox = ttk.Combobox(
            self.frame,
            textvariable=self.search_query_type,
            width=25,
            state='readonly',
            values=values
        )
        self.combobox.current(0)
        self.combobox.grid(column=2, row=1, sticky=(W, E))
        ttk.Label(self.frame, text='Upload Your CSV (*.csv) File:',font = "Times 12 bold").grid(column=2, row=5, sticky=W)
        # Add a button to log the message
        self.button = ttk.Button(self.frame, text='Upload File', command=self.submit_message)
        self.button.grid(column=2, row=7, sticky=W)
        ttk.Label(self.frame, text='[*] Excel File Must Contain These Exact Columns & Names',font = "Times 8 italic").grid(column=2, row=8, sticky=W)
        ttk.Label(self.frame, text='[*] BRAND || SKU || UPC || TITLE',font = "Times 8 italic",foreground="red").grid(column=2, row=9, sticky=W)
        self.button2 = ttk.Button(self.frame, text='Run Bot', command=self.run_bot)
        self.button2.grid(column=2, row=11, sticky=W)



    def submit_message(self):
        # Get the logging level numeric value
        file_name = filedialog.askopenfilename(parent=self.frame,
                                    initialdir=os.getcwd(),
                                    title="Please select a file:",
                                    filetypes=my_filetypes)
        self.filename = file_name

    def run_bot(self):
        logger.log(logging.WARNING,'[*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*]')
        search_query_type_value = self.search_query_type.get()
        scraper_bot = ScraperBot()
        scraper_bot.start_driver()
        scraper_bot.run(self.filename,search_query_type_value)
        scraper_bot.close_driver()
        logger.log(logging.WARNING,'[*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*][*]')


class ThirdUi:

    def __init__(self, frame):
        self.frame = frame
        photo = PhotoImage(file="BBP.png")
        label = Label(self.frame,image=photo)
        label.image = photo
        label.grid(column=8, row=0, sticky="ne")
        ttk.Label(self.frame, text='This Software Is Made Under MIT License By Mobasshir Bhuiyan Shagor (mobasshirbhuiyan.shagor@gmail.com)                         ').grid(column=0, row=0, sticky=W)

class App:

    def __init__(self, root):
        self.root = root
        root.title('BuyBoxPro.com Scraper')
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        # Create the panes and frames
        vertical_pane = ttk.PanedWindow(self.root, orient=VERTICAL)
        vertical_pane.grid(row=0, column=0, sticky="nsew")
        horizontal_pane = ttk.PanedWindow(vertical_pane, orient=HORIZONTAL)
        vertical_pane.add(horizontal_pane)

        form_frame = ttk.Labelframe(horizontal_pane, text="File")
        form_frame.columnconfigure(1, weight=1)
        horizontal_pane.add(form_frame, weight=1)

        console_frame = ttk.Labelframe(horizontal_pane, text="Console")
        console_frame.columnconfigure(0, weight=1)
        console_frame.rowconfigure(0, weight=1)
        horizontal_pane.add(console_frame, weight=1)

        third_frame = ttk.Labelframe(vertical_pane, text="Info")
        vertical_pane.add(third_frame, weight=1)
        # Initialize all frames
        self.form = FormUi(form_frame)
        self.console = ConsoleUi(console_frame)
        self.third = ThirdUi(third_frame)
        self.root.protocol('WM_DELETE_WINDOW', self.quit)
        self.root.bind('<Control-q>', self.quit)
        signal.signal(signal.SIGINT, self.quit)

    def quit(self, *args):
        self.root.destroy()


def main():
    logging.basicConfig(level=logging.DEBUG)
    root = tk.Tk()
    app = App(root)
    app.root.mainloop()


if __name__ == '__main__':
    main()