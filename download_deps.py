import queue
import urllib.request
import os
import logging
import threading
from queue import Empty, Queue
from datetime import datetime
import shutil
import time
import subprocess
import yaml


NUM_PARALLEL_DOWNLOADS = 10

class Downloader(threading.Thread):
    def __init__(self, queue):
        super(Downloader, self).__init__()
        self.queue = queue
        self.complete = False

    @staticmethod
    def singleDownload(download_url: str, save_as: str, local_dir: os.PathLike):
        # make sure next item is valid
        try:
            if not download_url or not local_dir or not save_as:
                raise RuntimeError("Missing an argument to download")

            absFileName = os.path.join(local_dir, save_as)
            urllib.request.urlretrieve(download_url, filename = absFileName)
            logging.info("Downloaded {} successfully as {}".format(download_url, absFileName))
            return True

        except Exception as e:
            logging.warning("error downloading {} : {}".format(download_url, e))
            return False

    def run(self):
        try:
            while True:
                download_url, save_as, local_dir = self.queue.get(False)
                self.singleDownload(download_url, save_as, local_dir)
        except queue.Empty:
            self.complete = True
            return
        except Exception as e:
            logging.warning("error downloading resource: {}".format(e))
            self.run()
        

    def isComplete(self):
        return self.complete
    
class NIPackageDownloader():

    def __init__(self, root: os.PathLike, repo: str) -> None:
        self.downloaders = []
        self.root = root
        self.repo = repo

        # Make sure downloads dir exists
        self.download_dir = os.path.join(self.root, "downloads")
        if(not os.path.exists(self.download_dir)):
            os.mkdir(self.download_dir)

    def runPkgsData(self, data: dict, deploy_dir: os.PathLike):
        # run the cross comp packages first
        if(not os.path.exists(self.root)):
            os.mkdir(self.root)

        # download the root packages and make links
        self.__downloadPackages__(data["build"]["pkgs"], self.root) 
        self.__makeLinks__(data["build"]["links"], self.root)

        # make sure the deploy_packages dir is clean
        if os.path.exists(deploy_dir):
            shutil.rmtree(deploy_dir)

        os.mkdir(deploy_dir)
        
        # download the deploy packages and make any links
        self.__downloadPackages__(data["deploy"]["pkgs"], deploy_dir) 
        self.__makeLinks__(data["deploy"]["links"], deploy_dir)


    def __readPackageDef__(self, name: str, pkgFile: os.PathLike):
        with open(pkgFile) as packageFile:
            start = -1
            parseData = ""
            for num, line in enumerate(packageFile, 1):
                if(start >= 0):
                    parseData += line

                if("Package: {}\n".format(name) == line):
                    start = num
                    parseData += line

                if(start >= 0 and "Priority:" in line):
                    data = yaml.safe_load(parseData)
                    return data 

        logging.error("error finding package {} in file: {}".format(name, pkgFile))
        return {}
    
    def __makeLinks__(self, links: list, localDir: str):
        for link in links:
            target = os.path.join(localDir, link[0])
            source = os.path.join(localDir, link[1])
            if(not (os.path.islink(source) or os.path.isfile(source))):
                os.symlink(source, target)
    
    def __downloadPackages__(self, packages: list, dest_dir: os.PathLike):
        # download & parse the index file
        index_file_path = os.path.join(self.download_dir, "Index")
        Downloader.singleDownload(self.repo+"/Packages", "Index", self.download_dir)

        # pack the queue
        queue = Queue()
        for package_name in packages:
            data = self.__readPackageDef__(package_name, index_file_path)
            package_url = self.repo + "/" + data["Filename"]
            queue.put((package_url, package_name, self.download_dir))

        # Start downloads
        threads = []
        for _ in range(NUM_PARALLEL_DOWNLOADS):
            threads.append(Downloader(queue))
            threads[-1].start()

        # Wait for downloads to complete
        complete = True
        while(not complete):
            complete = True

            for downloader in threads:
                complete = complete and downloader.isComplete()

            time.sleep(0.01)

        # now unarchive the packages
        for package_name in packages:
            try:
                # wait for fs to stabilize
                time.sleep(0.1)

                # open the ipk and get its contents into the current dir
                archive = os.path.join(self.download_dir, package_name)
                exitCode = subprocess.run(["ar", "x", archive, "--output={}".format(self.download_dir)])
                if(not exitCode.returncode == 0):
                    raise RuntimeError(f"ar failed with return code {exitCode}")
                
                # rip the data.tar archive open and dump it into the local dir
                data_tar_file = os.path.join(self.download_dir, "data.tar.xz")
                shutil.unpack_archive(data_tar_file, dest_dir, "gztar")

            except Exception as e:
                logging.error(f"error unarchiving {package_name} : {e}")