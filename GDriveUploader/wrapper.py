import httplib2
import time
import hashlib
import re
import mimetypes
import os
import uuid
import logging

from tlog import TransactionLog
from apiclient.discovery import build
from oauth2client import client
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.http import MediaFileUpload

FOLDER_TYPE = 'application/vnd.google-apps.folder'

logger = logging.getLogger('GDriveUploader.wrapper')

class GDriveWrapper(object):

    def __init__(self, tr_log_file, user_email, service_email, sercive_key_file):
        self.service = self._createDriveService(user_email, service_email, sercive_key_file)
        self.transaction_log = tr_log_file 
        self.sid = uuid.uuid1() # session id

    def __del__(self): 
        self.transaction_log.close()

    def _createDriveService(self, user_email, service_email, sercive_key_file):
      """Build and returns a Drive service object authorized with the service accounts
      that act on behalf of the given user.

      Returns:
        Drive service object.
      """
      f = file(sercive_key_file, 'rb')
      key = f.read()
      f.close()
      credentials = SignedJwtAssertionCredentials(service_email, key,
          scope='https://www.googleapis.com/auth/drive', sub=user_email)
      http = httplib2.Http()
      http = credentials.authorize(http)
      return build('drive', 'v2', http=http)

    def _checksum_md5(self, filename):
        md5 = hashlib.md5()
        with open(filename,'rb') as f: 
            for chunk in iter(lambda: f.read(8192), b''): 
                md5.update(chunk)
        return md5.hexdigest()

    def stat(self, parent_id, title):
        """ stat object by name (title) in the parent folder """
        with TransactionLog(self.sid, self.transaction_log, 'stat', parent_id, title) as logline:
            param = {}
            param['q'] = "'%s' in parents and title='%s' and trashed=false" % (parent_id, title)
            param['fields'] = 'items(id,mimeType,fileSize,md5Checksum)'
            responce = self.service.files().list(**param).execute()
            id = str(responce['items'][0]['id'])
            mimeType = str(responce['items'][0]['mimeType'])
            logline['child'] = id
            logline['mimeType'] = mimeType
            if FOLDER_TYPE == mimeType:
                return True, (id, mimeType, None, None)
            else:
                return True, (id, mimeType, int(responce['items'][0]['fileSize']), responce['items'][0]['md5Checksum'])
        return False, ''        
        
    def upload(self, local_filename, parent_id, fileTitle):  
        """ Upload file into specific parent dir 
            stat GDrive object with same name already exists 
            - if target object exists and it is a file, then source should be has different {MD5,size}. When localfile{size,md5} == targetfile{size,md5} -> file considered duplicated and excluded from upload.
            - if target object is a directory -> we rejecting upload
        """
        with TransactionLog(self.sid, self.transaction_log, 'update', parent_id, fileTitle) as logline:
            logline['source'] = local_filename
            rc, stat = self.stat(parent_id, fileTitle)

            if rc: 
                logline['child'] = stat[0]
                logline['mimeType'] = stat[1]
                if stat[1] == FOLDER_TYPE:
                    logger.debug("Unable upload file %s in parent id %s. Directory with the same name already exists." % (fileTitle, parent_id))
                    logline['status'] = TransactionLog.STATUS_CONFLICT
                    return False, ''
                elif stat[2] == os.stat(local_filename).st_size and stat[3] == self._checksum_md5(local_filename):
                    """ file exists, check for duplication """
                    logger.debug("File '%s' on local file system equal to size and md5 of object '%s' stored in GDrive. Skipping upload." % (local_filename, parent_id))
                    logline['status'] = TransactionLog.STATUS_NOT_MODIFIED
                    logline['md5'] = stat[3]
                    logline['size'] = stat[2]
                    return rc, stat
                
                
            mimeType = mimetypes.guess_type(local_filename)[0]
            if mimeType is None: mimeType = 'application/octet-stream'
            payload = MediaFileUpload(local_filename, mimeType)
            body = {
                'title'      : fileTitle,
                #'description': 'automatic uploading',
                'mimeType'   : mimeType ,
                'parents'    : [{'id': parent_id}]
            }    
            if rc:
                """ file exists, update """
                logline['op'] = 'update'
                responce = self.service.files().update(fileId=stat[0], fields='id,mimeType,fileSize,md5Checksum', body=body, media_body=payload).execute()
            else:           
                logline['op'] = 'insert'
                responce = self.service.files().insert(fields='id,mimeType,fileSize,md5Checksum', body=body, media_body=payload).execute()
            logline['child'] = responce['id']
            logline['mimeType'] = responce['mimeType']
            logline['size'] = logline['bytes'] = int(responce['fileSize'])
            logline['md5'] = responce['md5Checksum']
            assert(FOLDER_TYPE != mimeType)
            return True, (responce['id'], responce['mimeType'], int(responce['fileSize']), responce['md5Checksum'])
        return False, ''        


    def dirlist(self, parent_id):
        """
        Returns 
            list of objects 
                file will consists md5, 
                folders will have mimeType set to 'application/vnd.google-apps.folder'
        {
         "items": [
          {
           "id": "0B9DmNepPcca_ZDJHQ1VOT21TTzQ",
           "title": "DSC_3005.zip",
           "mimeType": "application/zip",
           "md5Checksum": "6bdf710cb1c2c046712f5bec7941277e"
          },
          {
           "id": "0B9DmNepPcca_X3FPX2YyOGFBT1U",
           "title": "Sacramento",
           "mimeType": "application/vnd.google-apps.folder"
          },
          ...
        }  
        """
        result = None
        page_token = None
        param = {}
        param['q'] = "'%s' in parents and trashed=false" % parent_id    
        param['fields'] = 'items(id,mimeType,title,fileSize,md5Checksum)'
        while True:
            if page_token:
                param['pageToken'] = page_token
            responce = self.service.files().list(**param).execute()
            if result is None: result = responce
            else: result['items'] += responce['items']
            page_token = responce.get('nextPageToken')
            if not page_token:  break
        return result
    
    #do_dirlist(service,'0B9DmNepPcca_MkJ5LUhEV2syaXc')    

    def mkdir(self, parent_id, dirTitle):        
        with TransactionLog(self.sid, self.transaction_log, 'mkdir', parent_id, dirTitle) as logline:
            rc, stat = self.stat(parent_id, dirTitle)
            if rc:
                logline['child'] = stat[0]
                logline['mimeType'] = stat[1]
                if stat[1] == FOLDER_TYPE:
                    """ directory already exists """
                    logline['status'] = TransactionLog.STATUS_NOT_MODIFIED
                    return rc, stat           
                else:
                    logger.debug("Unable create directory %s in parent id %s. File with the same name already exists." % (dirTitle, parent_id))
                    logline['status'] = TransactionLog.STATUS_CONFLICT
                    return False, ''
            
            # directory not exists, try to create one
            body = {
                'title'   : dirTitle,
                'mimeType': FOLDER_TYPE
            }    
            if len(parent_id):
                body['parents']=[{'id': parent_id}]
            else:
                body['isRoot']=True
            responce = self.service.files().insert(fields='id,mimeType', body=body).execute()
            logline['child'] = responce['id']
            logline['mimeType'] = responce['mimeType']
            assert(responce['mimeType'] == FOLDER_TYPE)
            return True, (responce['id'], responce['mimeType'], None, None) # same response as stat()
        return False, ''     
    
    def mkpath(self, parent_id, dirPath):   
        """ check whether each component exists in component path 
            create fodlers on GDrive when component does not exist
            fails if folder can't be created
        """    
        components = filter(None, re.split( r'[\\/]', dirPath))
        for dirTitle in components:
            rc, stat = self.mkdir(parent_id, dirTitle)
            if not rc:
                return rc, stat
            parent_id = stat[0]

        """ return status of last mkdir operation """            
        return rc, stat            

    def bad_source(self, source, parent_id):
        with TransactionLog(self.sid, self.transaction_log, 'bad_source', parent_id, '-') as logline:
            logline['source'] = source
            logline['status'] = TransactionLog.STATUS_UNSUPPORTED_MEDIA


