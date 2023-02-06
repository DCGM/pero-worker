#!/usr/bin/env python3

# Cache for managing local OCR files in PERO worker

import os
import configparser
import tarfile
import zipfile

class OCRFileCache:
    """
    Manages local OCR files based on stage name and version
    """
    def __init__(self, cache_dir='/tmp/pero-cache', auto_cleanup=True):
        """
        Init the cache.
        :param cache_dir: top level cache directory where all files will be stored
        :param auto_cleanup: if True all files are automatically removed when cache object is deleted
        """
        self.cache_dir = cache_dir
        self.auto_cleanup = auto_cleanup
        # contains version for each stored stage
        self.stored_stages = {}
        
        # load existing cache
        if os.path.exists(cache_dir):
            for stage in os.listdir(cache_dir):
                ocr_version_path = os.path.join(self.cache_dir, stage, 'version.txt')
                if os.path.isfile(ocr_version_path):
                    with open(ocr_version_path, 'r') as version_file:
                        self.stored_stages[stage] = version_file.read()
                else:
                    self.clean_cached_files(os.path.join(self.cache_dir, stage))
        else:
            self.create_cache_dir()

    def get_stage_data_path(self, stage):
        """
        Returns ocr configuration directory path.
        :param stage: stage name
        :return: config directory path
        :raise ValueError if stage is not in cache
        """
        if stage not in self.stored_stages:
            raise ValueError('No record for given stage!')
        return os.path.join(self.cache_dir, stage)
    
    def get_stage_version(self, stage):
        """
        Returns ocr stage configuration version.
        :param stage: stage name
        :return: stage version
        :raise ValueError if stage is not in cache
        """
        if stage not in self.stored_stages:
            raise ValueError('No record for given stage!')
        return self.stored_stages[stage]
    
    def get_stage_config(self, stage):
        """
        Returns ocr stage config.ini file content.
        :return: stage configuration (dict)
        :raise ValueError if stage is not in cache
        """
        if stage not in self.stored_stages:
            raise ValueError('No record for given stage!')
        config = configparser.ConfigParser()
        config.read(os.path.join(self.cache_dir, stage, 'config.ini'))
        return config
    
    @staticmethod
    def unpack_archive(archive, path):
        """
        Unpacks tar or zip archive
        :param archive: path to archive to unpack
        :param path: path where archive will be unpacked to
        :raise tarfile.TarError if Attempted Path Traversal in Tar File is detected
        :raise tarfile.TarError if tar file can't be read
        :raise tarfile.TarError if Attempted Path Traversal attack detected in Tar File
        :raise OSError: if fails to write to filesystem
        :raise zpirfile.BadZipfile: if zipfile can't be read
        """
        def is_within_directory(directory, target):
            abs_directory = os.path.abspath(directory)
            abs_target = os.path.abspath(target)

            prefix = os.path.commonprefix([abs_directory, abs_target])

            return prefix == abs_directory
        
        def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            for member in tar.getmembers():
                member_path = os.path.join(path, member.name)
                if not is_within_directory(path, member_path):
                    raise tarfile.TarError("Attempted Path Traversal in Tar File")

            tar.extractall(path, members, numeric_owner=numeric_owner)
        
        if tarfile.is_tarfile(archive):
            with tarfile.open(archive, 'r') as arch:
                safe_extract(arch, path)
        else:
            with zipfile.ZipFile(archive, 'r') as arch:
                arch.extractall(path)
    
    def add_stage(self, stage, config, version, sftp_path, sftp_client):
        """
        Adds new stage to cache.
        :param stage: stage name
        :param config: stage config.ini content
        :param version: current stage version
        :param sftp_path: path to stage ocr model on sftp server
        :param sftp_client: initialized client for getting data from sftp server
        :raise zipfile.BadZipFile when received archive is not properly formated zipfile
        :raise zipfile.LargeZipFile when a ZIP file would require ZIP64 functionality but that has not been enabled
        :raise tarfile.TarError when tar file cannot be extracted
        :raise ConnectionError if sftp connection fails
        :raise FileNotFoundError if file is not found on sftp server
        """
        ocr_path = os.path.join(self.cache_dir, stage)
        ocr_version_path = os.path.join(ocr_path, 'version.txt')
        ocr_config_path = os.path.join(ocr_path, 'config.ini')
        archive_path = os.path.join(self.cache_dir, f'{stage}.archive')

        if stage in self.stored_stages:
            self.remove_stage(stage)

        if sftp_path:
            # get ocr model from sftp server and save it to archive_path
            sftp_client.sftp_get(f'{sftp_path}', archive_path)
            
            # unpack archive with model to ocr_path folder
            self.unpack_archive(archive_path, ocr_path)

            # remove archive after unpacking
            self.clean_cached_files(archive_path)
        else:
            os.mkdir(ocr_path)

        # save config
        with open(ocr_config_path, 'w') as config_file:
            config_file.write(config)
        
        # save version
        with open(ocr_version_path, 'w') as version_file:
            version_file.write(version)
        
        # set version
        self.stored_stages[stage] = version
    
    def remove_stage(self, stage):
        """
        Removes stage data from cache.
        :param stage: stage name
        """
        if stage not in self.stored_stages:
            return
        
        self.clean_cached_files(os.path.join(self.cache_dir, stage))
        self.stored_stages.pop(stage, None)

    def create_cache_dir(self):
        """
        Creates cache directory
        """
        if not self.cache_dir:
            return
        
        if os.path.isdir(self.cache_dir):
            return
        
        os.makedirs(self.cache_dir)
    
    def clean_cached_files(self, path=None):
        """
        Removes cached files and directories recursively
        :param path: path to file/directory to clean up
        """
        # default path = top level cache directory
        if not path:
            path = self.cache_dir
        
        # if cache directory is not set - nothing to clean
        if not path:
            return
        
        # path is not valid
        if not os.path.exists(path):
            return
        
        # remove file
        if os.path.isfile(path):
            os.unlink(path)
            return
        
        # clean files from directory
        for file_name in os.listdir(path):
            self.clean_cached_files(os.path.join(path, file_name))
        
        # remove directory
        os.rmdir(path)
    
    def __del__(self):
        """
        Cache destructor, removes all files from cache if 'auto_cleanup' is set to True
        """
        if self.auto_cleanup:
            self.clean_cached_files()
