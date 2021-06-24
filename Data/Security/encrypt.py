import pyaes
from Data.Config import pgconfig
from types import SimpleNamespace

import boto3
import json
import base64
import hashlib
import string
import random
import os
#from Crypto import Random
#from Crypto.Cipher import AES
import base64
import pyAesCrypt
from Data.Utils import pgdirectory


class SecurityPass:
    def __init__(self, *, db_system, postfix):
        self._parameter = pgconfig.Config().setup_secure_pass(db_system, postfix)

        self._parameter.data_home = pgconfig.Config().setup_data_home()

        print(self._parameter)

    def gen_encryption(self, *, ptext: str, filepath: str, key: str):
        key_str = key.encode('utf-8')
        with open(filepath, 'wb') as file:
            file.write(self.encrypt(keypass=key_str, ptext=ptext))

    def gen_encrypt(self, *, ptext: str, entity: str):
        return self.gen_encryption(ptext=ptext,
                                   filepath=str(self._parameter.data_home.data_home) + '/.' + str(self._parameter.system_secret) + '.' + entity,
                                   key=self._parameter.system_keypass)

    def gen_decryption(self, *, key: str, filepath: str):
        key_str = key.encode('utf-8')
        with open(filepath, 'rb') as file:
            return self.decrypt(keypass=key_str, ptext=file.read())

    def gen_decrypt(self, *, entity):
        return self.gen_decryption(filepath=str(self._parameter.data_home.data_home) + '/.' + str(self._parameter.system_secret) + '.' + entity,
                                   key=self._parameter.system_keypass)


    @staticmethod
    def encrypt(*, keypass, ptext):
        aes = pyaes.AESModeOfOperationCTR(keypass)
        return aes.encrypt(ptext)

    @staticmethod
    def decrypt(*, keypass, ptext):
        aes = pyaes.AESModeOfOperationCTR(keypass)
        return aes.decrypt(ptext).decode('utf-8')


if __name__ == '__main__':
    ptext = "admin123"
    entity = 'meta'
    sp = SecurityPass(db_system='meta', postfix='')
    sp.gen_encrypt(ptext=ptext, entity=entity)
    print(sp.gen_decrypt(entity=entity))

    exit(0)





    text = 'Zaqwsx567$$'
    key_str = 'LyfZTeBjLOmjTsoq'

    key = key_str.encode('utf-8')
    print(key)
    encrypted_text = Encrypt(key, text)
    print(encrypted_text)

    with open('/Users/jianhuang/PycharmProjects/Data/Data/Security/LyfZTeBjLOmjTsoq.txt', 'wb') as f:
        f.write(encrypted_text)

    with open('/Users/jianhuang/PycharmProjects/Data/Data/Security/LyfZTeBjLOmjTsoq.txt', 'rb') as f:
        encrypted_text = f.read()

    decoded_text = Decrypt(key, encrypted_text)
    print(decoded_text)
    assert(text == Decrypt(key, encrypted_text))
