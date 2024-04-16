import datetime
from time import sleep

class API_Simulator():

    def _download_rates(self, extract: datetime, date: datetime) -> str:
        """
        Download file and returns local file path where file is
        downloaded.
        """
        sleep(1) # simulates API request time.
        return 'storage/{}/{}.csv'.format(extract.strftime('%Y-%m-%d'), date.strftime('%Y-%m-%d'))
    

