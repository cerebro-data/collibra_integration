import threading
import time
from concurrent.futures import ThreadPoolExecutor
import _thread

from app_manager.utility.common.singleton import Singleton
from app_manager.app_config_manager import ConfigManager


class ThreadManager(metaclass=Singleton):

    def __init__(self):
        self.thread_config = ConfigManager().get_thread_config()
        # print(self.thread_config["max_threads"])
        self.thread_executor = None

    def get_max_workers(self):
        return int(self.thread_config["max_worker"])

    def get_thread_executor(self, max_thread_pool, thread_name):
        return ThreadPoolExecutor(max_workers=int(max_thread_pool), thread_name_prefix=thread_name)

    def create_indepentent_thread(self, function, args=()):
        _thread.start_new_thread(function, args)

    def create_process_thread(self, target):
        self.thread_executor.submit(target)

    def get_main_thread(self):
        return threading.currentThread()

    def get_active_threads(self):
        return threading.enumerate()

    def wait(self, secs):
        time.sleep(secs)

    def print_progress(self):

        for thread in threading.enumerate():
            print(f"{thread.name}")

    def join_active_threads(self):
        for thread in threading.enumerate():
            if thread == threading.currentThread():
                continue
            thread.join()