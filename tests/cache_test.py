#!/usr/bin/env python3

from worker.cache import OCRFileCache
import sys
import os

TEST_ARCH = 'tests/cache_test_data/test1.tar.xz'
CACHE_DIR = './tests/tmp'
CONFIG_CONTENT = '[TEST]\ntest_key=test value\n'
TEST_STAGE_NAME = 'test1'
STAGE_DATA_DIR = os.path.join('./tests/tmp', TEST_STAGE_NAME)

class dummySFTPclient():
    """
    Copy test archive to cache dir, provides api used by OCRFileCache to download file from sftp store.
    """
    def sftp_get(self, remote_path, local_path):
        with open(local_path, 'wb') as out_file:
            with open(TEST_ARCH, 'rb') as in_file:
                out_file.write(in_file.read())

def main():
    # set auto_cleanup to False to keep CACHE_DIR after test
    cache = OCRFileCache(CACHE_DIR, True)

    cache.add_stage(TEST_STAGE_NAME, CONFIG_CONTENT, '1', '/x/y/z', dummySFTPclient())
    print(f'Stage name: test1')
    print(f'Stage version: {cache.get_stage_version(TEST_STAGE_NAME)}')
    print(f'Stage data path: {cache.get_stage_data_path(TEST_STAGE_NAME)}')
    config = cache.get_stage_config(TEST_STAGE_NAME)
    print('Stage config content:')
    for key in config:
        print(f'[{key}]')
        for var in config[key]:
            print(f'{var}={config[key][var]}')
    
    assert os.listdir(STAGE_DATA_DIR) == ['test_document.txt', 'config.ini', 'version.txt']
    assert len(list(cache.stored_stages.keys())) == 1
    assert list(cache.stored_stages.keys()) == [TEST_STAGE_NAME]
    assert cache.get_stage_version(TEST_STAGE_NAME) == '1'
    del cache  # test auto_cleanup
    assert os.path.exists(CACHE_DIR) == False

    return 0

if __name__ == "__main__":
    sys.exit(main())
