from datetime import datetime,timezone,timedelta
import os
import sys

class UshmUtils():
    def __init__(self):
        self.time = datetime.now()

    # Helper functions.
    def timestamp(self,year=False,tz=None):
        """
        Return the current time as a string. Default timezone(tz) uses local time. 
        Use tz=timezone.utc to return UTC time
        Args:
            dt_format (str): desired format of returned timestamp (if error, return default format)
            tz (str): desired timezone of timestamp (if error, return UTC time)
        Returns:
            dt_string (str): string representation of current datetime
            dt_now (datetime): datetime object
        """
        if year:
            dt_format="%Y-%m-%d %H:%M:%S"
        else:
            dt_format="%H:%M:%S"
            
        try:
            dt_now = datetime.now(tz)
            dt_string = dt_now.strftime(dt_format)
        except:
            dt_now = datetime.now(timezone.utc)
            dt_string = dt_now.strftime("%Y-%m-%d %H:%M:%S")
        
        return dt_string#, dt_now

if __name__ == "__main__":
    pass
