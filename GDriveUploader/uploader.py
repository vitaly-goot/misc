import sys
import os
import traceback
import time
import logging, logging.config
import json

from Queue import Queue
from wrapper import GDriveWrapper
from threading import Thread,current_thread

USER_EMAIL='vitaly.goot@gmail.com'

"""Email of the Service Account"""
SERVICE_ACCOUNT_EMAIL = '430571788972-e453it12vp1h4p56rji9m5h2b9pgdfuj@developer.gserviceaccount.com'

"""Path to the Service Account's Private Key file"""
SERVICE_ACCOUNT_PKCS12_FILE_PATH = 'uploader-eebc72f9df5d.p12'

with open('logging.json', 'rt') as f:
    config = json.load(f)
logging.config.dictConfig(config)
logger = logging.getLogger('GDriveUploader')

class Dispatcher(Queue):
    def __init__(self, gdrive):
        Queue.__init__(self)
        self.gdrive = gdrive

    def processDir(self, sourceDir, targetDirId, recursive=False):
        if not os.path.exists(sourceDir):
            self.gdrive.bad_source(sourceDir, targetDirId) # reporting to transaction log
            logger.debug("Path %s does not exists." % sourceDir)
            return
            
        for sourceTitle in os.listdir(sourceDir):
            source = os.path.join(sourceDir, sourceTitle)
            if os.path.isfile(source): 
                """ object on local file system is a file, add it to queue 
                    multiple workers can be attached to that queue and execute upload task concurrently
                """
                logger.debug("Schedule upload for %s to %s:%s" % (source, targetDirId, sourceTitle))
                self.put((source, targetDirId, sourceTitle)) 
            elif os.path.isdir(source):
                (rc, stat) = self.gdrive.mkdir(targetDirId, sourceTitle)
                if recursive:
                    if rc:
                        newTargetDirId = stat[0]
                    else:     
                        """ continue for logging purposes. Further attempt to invoke GDriveWrapper API (mkdir, upload etc...)
                        with parentId=None should emit error log line """                    
                        newTargetDirId = None
                    logger.debug("Walking down os tree to new horizonts %s:%s" % (source, newTargetDirId))
                    self.processDir(source, newTargetDirId, recursive) 
                pass
            else:
                """ *** TODO ***
                    add support for symbolic links 
                    it's easy to implement immediate link to file, 
                    while link to link or link to dir requires more sophisticated handling 
                    e.g. consider symlink /a/b -> /a
                         or link to link which goes in loop /a/b -> /x/y/z -> /a
                """
                self.gdrive.bad_source(source, targetDirId) # reporting to transaction log
                logger.debug("Detected symbolic link [%s]. Symbolic links are not supported just yet." % source)
            pass
        pass # for 
    pass
  
def worker(dispatcher, tlog, **kwargs):
    gdrive = GDriveWrapper(tlog, kwargs['user_mail'], kwargs['service_mail'], kwargs['key'])
    while True:
        uploadTask = dispatcher.get()
        try:
            logger.debug(uploadTask)
            gdrive.upload(*uploadTask)
        except:
            traceback.print_exc(file=sys.stderr)
        finally:
            dispatcher.task_done()

def main():
    """ transaction log file """
    tlog = open("transaction.log", "a")

    with open('uploader.json', 'rt') as f:
        config = json.load(f)

    nworkers = config['nworkers']
    if nworkers < 0: 
        logger.debug("Invalid nworkers argument %d" % nworkers)
        nworkers = 1
    elif nworkers > 20: 
        logger.debug("Invalid nworkers argument %d" % nworkers)
        nworkers = 20
    logger.debug("nworkers set to %d" % nworkers)

    gdrive = GDriveWrapper(tlog, config['user_mail'], config['service_mail'], config['key'])
    dispatcher = Dispatcher(gdrive)

    while True:
        time.sleep(1)

    for dir in config['directory']:
        rc, stat = gdrive.mkpath('root', dir['targetDir'])
        if rc:
            dispatcher.processDir(dir['sourceDir'], stat[0],  'yes' == dir['recursive'])

    """ TODO implement dispatcher.processFile """



    """ starting worker threads and proceed with upload """
    for j in range(nworkers):
        t = Thread(target=worker, args=(dispatcher,tlog), kwargs=config)
        t.daemon = True
        t.start()

    dispatcher.join()       # block until all tasks are done

    tlog.close()

def unitTest():
    # create "drv_root:1/2/3" path (if not already exists)
    (rc, stat) = gdrive.mkpath('root', "1/2/3")
    assert(rc)
    print(stat)

    # create 4/5/6 relatively to "drv_root:1/2/3" path (if not already exists)
    subdir_id = stat[0]
    (rc, stat) = gdrive.mkpath(subdir_id, "4/5/6")
    assert(rc)
    print(stat)

    # create 7 relatively to "drv_root:1/2/3/4/5/6" path (if not already exists)
    subdir_id = stat[0]
    (rc, stat) = gdrive.mkdir(subdir_id, "7")
    assert(rc)
    print(stat)


    # upload file 'test' as '8' (actually upload and rename)
    subdir_id = stat[0]

    file = open("test", "w")
    file.write("I've seen the other side of rainbow. It was black and white. It was black and white.")
    file.flush()
    file.close

    (rc, stat) = gdrive.upload('test', subdir_id, '8')
    assert(rc and 84 == int(stat[2]) and '1a6da41da9916bbcbe660978b1e6b635' == stat[3])
    print(stat)

    # same file upload should be ignored 
    (rc, stat) = gdrive.upload('test', subdir_id, '8')
    assert(rc)
    print(stat)

    #  modify file and store it again (shuold be accepted)
    file = open("test", "w")
    file.write("I've seen the other side of rainbow. It was black and white. It was black and white!")
    file.flush()
    file.close
    (rc, stat) = gdrive.upload('test', subdir_id, '8')
    assert(rc and 84 == int(stat[2]) and 'd1d61d70ba15e210a37ea07b8940936d' == stat[3])
    print(stat)

    # try to create directory '8' while file '8' already exists
    (rc, stat) = gdrive.mkdir(subdir_id, "8")
    assert(not rc)

if __name__ == "__main__":
    main()

    sys.exit(0)

