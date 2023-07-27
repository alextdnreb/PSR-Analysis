import scrapy
from scrapy_playwright.page import PageMethod
import time

class YoutubeSpider(scrapy.Spider):
    name = "youtube"

    def __init__(self, channel, **kwargs):
        super().__init__(**kwargs)

        self.channel = channel

    def start_requests(self):
        """
            start on youtube.com, because direct links to channels 
            result in a redirect to accept cookies
        """
        cookies_request = scrapy.Request("https://www.youtube.com",
                            meta=dict(
                                playwright = True,
                                playwright_include_page = True,
                                playwright_page_methods = [PageMethod('wait_for_selector', 'button[aria-label="Accept the use of cookies and other data for the purposes described"]')]
                            ),
                            callback=self.accept_cookies)
        
        yield cookies_request

    async def accept_cookies(self, response):
        """
            accept cookies; afterwards request channel page
        """
        page = response.meta["playwright_page"]
        await page.click('button[aria-label="Accept the use of cookies and other data for the purposes described"]')
        url = f"https://www.youtube.com/{self.channel}/videos"
        yield scrapy.Request(url=url, meta=dict(
                            playwright = True,
                            playwright_include_page = True,
                            playwright_page_methods = [PageMethod('wait_for_selector', '#contents')]
                        ), callback=self.parse)

    async def parse(self, response):
        """
            parse channel page for video IDs
        """
        page = response.meta["playwright_page"]

        count = await page.evaluate("""
            document.querySelector("#videos-count :first-child").textContent
        """)
        video_count = self.count_to_number(count)
        while True:
            # get all video elements and extract their video IDs
            video_ids = await page.evaluate("""
                Array.from(document.querySelectorAll('#video-title-link')).map(video => video.href).map(url => url.split("?v=")[1])
            """)
            number_of_videos = float(len(video_ids))

            print(number_of_videos)
            print(video_count)
            # this condition does not (always) work as exit condition
            # the video count of youtube appears not to be accurate
            # adjust NUMBER_TO_SCRAPE manually to the number of the channel's videos
            NUMBER_TO_SCRAPE = 280
            if number_of_videos >= video_count or number_of_videos == NUMBER_TO_SCRAPE: 
                break
            else:
                await page.evaluate("window.scrollBy({top: document.querySelector('#content').scrollHeight,left: 0,behavior: 'smooth'})")
                # wait until new videos are loaded, then repeat scrolling
                time.sleep(3)
            

        # yield the video IDs as items for further processing
        for video_id in video_ids:
            yield {"video_id": video_id, "name": self.channel}
    

    def count_to_number(self, count):
        """ 
            transform rounded video counts to actual numbers
            example: 1k subscribers -> 1000 
        """
        count = count.replace(" subscribers", "")
        MILLION = "M"
        THOUSAND = "K"
        if MILLION in count:
            count = count.replace(MILLION, "")
            count = float(count) * 10000000
        elif THOUSAND in count:
            count = count.replace(THOUSAND, "")
            count = float(count) * 1000
    
        return float(count)