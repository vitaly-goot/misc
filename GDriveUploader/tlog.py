import socket
import time
import os
import sys
import logging

from googleapiclient.errors import HttpError

logger = logging.getLogger('GDriveUploader.tlog')

""" Context controled log """
class TransactionLog():

    """ 
    report HTTP status code 
    failed HTTP responce (googleapiclient.errors.HttpError exception) we intercept in TransactionLog.__exit__() below
    also we piggy back our own codes on top of HTTP status code. For example, STATUS_NOT_MODIFIED can be reported by our GDriveWrapper since we add posix semantic to mkdir or stat ops
    see http://en.wikipedia.org/wiki/List_of_HTTP_status_codes#4xx_Client_Error
    """
    STATUS_SUCCESS = 200 # set to default in constructor
    STATUS_NOT_MODIFIED = 304 
    STATUS_NOT_FOUND = 404
    STATUS_CONFLICT = 409
    STATUS_UNSUPPORTED_MEDIA = 417
    STATUS_UNHANDLED_EXCEPTION = 500 # this one will be reported by TransactionLog for all unhandled exceptions

    """ Transaction log file format:
        STARTTIME MACHINE_IP PROCESS_ID SESION_ID STATUS DURATION OP BYTES_TRANSFERED SIZE MD5 SOURCE PARENT CHILD MIME_TYPE TITLE
        Where,
            OP ::= 'mkdir' | 'stat' | 'insert' | 'update'
            
    """
    def __init__(self, sid, file, op, parent, title):
        self.file = file
        self.ip=self._myip()
        self.pid=os.getpid()
        self.sid=sid
        
        self.line = {
            'starttime' : 0,
            'duration'  : 0,
            'ip'        : self.ip,
            'pid'       : self.pid,
            'sid'       : self.sid,
            'op'        : op,
            'status'    : TransactionLog.STATUS_SUCCESS, 
            'bytes'     : 0,
            'size'      : 0,
            'md5'       : '-',
            'source'    : '-',
            'parent'    : parent,
            'child'     : '-',
            'mimeType'  : '-',
            'title'     : title
        }

    def __enter__(self):
        self.line['starttime'] = time.time() * 1000
        return self.line

    def __exit__(self, type, value, tb):
        if value is not None:
            """ 
                We silently masking all exceptions inside 'with TransactionLog ....'  body. 

                    with TransactionLog as line
                        ...
                        !!! exceptios are masked in this section !!!
                        ...
                    pass
            """       


            """ Exception handling """
            if type == HttpError:
                """ HTTP responces from GDrive server that end up badly should land up here
                    we taking HTTP status code stright from the server responce 
                """
                self.line['status'] = value.resp['status']
                logger.debug("Exception masked by TransactionLog:", exc_info=(type, value, tb))
            elif type == IndexError:
                """ HTTP server responce was good but we did not get what we asked 
                    Usually means we can't parse out of server responce requested properties (e.g. responce['id'])
                    Google server does not respond 404 since we run search query which just returns empty.
                    So far, we emulate 404 here.
                """    
                self.line['status'] = TransactionLog.STATUS_NOT_FOUND 
                logger.debug("Exception masked by TransactionLog:", exc_info=(type, value, tb))
            else:
                """ put status code 500 for all other unhandled exceptions 
                    going forward, if we will see 500 in the transaction log 
                    we should enable debug output, figure out which exception caused that 500
                    and add another handler above with appropriated error code
                """
                self.line['status'] = TransactionLog.STATUS_UNHANDLED_EXCEPTION 
                logger.error("Unhandled exception masked by TransactionLog:", exc_info=(type, value, tb))

        self.line['duration'] = (time.time() * 1000) - self.line['starttime']
        """              1  2  3  4  5  6  7  8  9  10 11 12 14 15 16"""
        self.file.write("%d %s %d %s %s %d %s %d %d %s %s %s %s %s %s\n" % 
                    (self.line['starttime'],  
                    self.line['ip'],
                    self.line['pid'],
                    self.line['sid'],
                    self.line['status'],      
                    self.line['duration'],    
                    self.line['op'],         
                    self.line['bytes'],
                    self.line['size'],
                    self.line['md5'],
                    self.line['source'],      
                    self.line['parent'],
                    self.line['child'],
                    self.line['mimeType'],
                    self.line['title']))

        # mask all exception 
        return True

    def _myip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 0))  # connecting to a UDP address doesn't send packets
        return s.getsockname()[0]
