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

    def singleDownload(self, download_url: str, save_as: str, local_dir: os.PathLike):
        # make sure next item is valid
        try:
            if not download_url or not local_dir or not save_as:
                raise RuntimeError("Missing an argument to download")

            absFileName = os.path.join(local_dir, "downloads", save_as)
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
                    #print("discovered package def")

                if(start >= 0 and "Priority:" in line):
                    data = yaml.safe_load(parseData)
                    return data 

                #if("Package: {}\n".format(name) in line):
                #        print("'" + line + "'")
        
        logging.error("error finding package {} in file: {}".format(name, pkgFile))
        return {}
    
    def makeLinks(self, links: list, localDir: str):
        for link in links:
            target = os.path.join(localDir, link[0])
            source = os.path.join(localDir, link[1])
            if(not (os.path.islink(source) or os.path.isfile(source))):
                os.symlink(source, target)
    
    def downloadPackages(self, packages: list):
        # download & parse the index file
        index_file_path = os.path.join(self.download_dir, "Index")
        Downloader().singleDownload(self.repo+"/Packages", "Index", self.download_dir)

        # pack the queue
        queue = Queue()
        for package_name in packages:
            data = self.__readPackageDef__(package_name, index_file_path)
            package_url = self.repo + data["Filename"]
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
        
        print("All files downloaded, unarchiving")
        time.sleep(0.5)

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
                shutil.unpack_archive(data_tar_file, self.root, "gztar")

                print(f"Unarchived {package_name} successfully")

            except Exception as e:
                logging.error(f"error unarchiving {package_name} : {e}")


if __name__ == "__main__":
    # Common vars used in path
    USER_HOME = os.path.expanduser('~')
    YEAR = str(datetime.date(datetime.now()).year)
    ARM_PREFIX = "arm-frc{}-linux-gnueabi".format(YEAR)
    CROSS_ROOT = os.path.join(USER_HOME, "wpilib", YEAR, "roborio", ARM_PREFIX)
    CWD = os.getcwd()

    pkg_dnldr = NIPackageDownloader(CROSS_ROOT, "")

    # download deps into cross root
    print("Downloading CC deps")
    buildDepsDir = CROSS_ROOT
    if(not os.path.exists(buildDepsDir)):
        os.mkdir(buildDepsDir)

    pkg_dnldr.downloadPackages(buildDeps["files"], buildDepsDir)
    pkg_dnldr.makeLinks(buildDeps["links"], buildDepsDir)

    print("All CC deps downloaded")

    # download deps for install
    print("Downloading deploy deps")
    deployDepDir = os.path.join(CWD, "extra_libs")
    if(not os.path.exists(deployDepDir)):
        os.mkdir(deployDepDir)
    else:
        shutil.rmtree(deployDepDir)
        os.mkdir(deployDepDir)

    pkg_dnldr.downloadPackages(deployDeps["files"], deployDepDir)
    pkg_dnldr.makeLinks(deployDeps["links"], deployDepDir)
    print("All deploy deps downloaded")

