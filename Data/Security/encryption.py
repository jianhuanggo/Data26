import pyaes
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

"""

# A 256 bit (32 byte) key
key = "This_key_for_demo_purposes_only!"
plaintext = "Text may be any length you wish, no padding is required"

# key must be bytes, so we convert it
key = key.encode('utf-8')

counter = pyaes.Counter(initial_value = 100)
aes = pyaes.AESModeOfOperationCTR(key, counter=counter)
ciphertext = aes.encrypt(plaintext)

# show the encrypted data
print (ciphertext)

# DECRYPTION
# CRT mode decryption requires a new instance be created

aes = pyaes.AESModeOfOperationCTR(key)

# decrypted data is always binary, need to decode to plaintext
decrypted = aes.decrypt(ciphertext).decode('utf-8')

# True
print (decrypted == plaintext)

"""


class Security:
    def __init__(self):
        self._token = None
        self._key = None

    @classmethod
    def Encrypt(cls, key, text):
        aes = pyaes.AESModeOfOperationCTR(key)
        return aes.encrypt(text)
        #ADDON.setSetting('email_pass_1', encrypted)

    @classmethod
    def Decrypt(cls, key, text):
        aes = pyaes.AESModeOfOperationCTR(key)
        #decrypted = aes.decrypt(ADDON.getSetting('email_pass_1'))
        return aes.decrypt(text).decode('utf-8')

    @classmethod
    def store_key(cls):
        token = os.urandom(32)
        #key = hashlib.md5(token).hexdigest()[:32].encode('utf8')
        #key = hashlib.md5(token).hexdigest()[:32]
        print(pgdirectory.workingdirectory())
        path = pgdirectory.workingdirectory()
        with open(path + '/' + '.pass', 'w') as f:
            f.writelines(str(token))

    @staticmethod
    def id_generator(size=16, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    @staticmethod
    def get_key_new():
        strn = Security.id_generator()
        return hashlib.md5(strn.encode('utf-8')).hexdigest()[:32]


    # encryption/decryption buffer size - 64K
    def encrypt_file(self, path, file_name):
        pyAesCrypt.encryptFile(path + file_name, path + file_name + ".aes", "GoScooT", 1024 * 64)

    def decrypt_file(self, path, file_name):
        pyAesCrypt.decryptFile(path + file_name + ".aes", path + file_name,  "GoScooT", 1024 * 64)


    def get_msgs(session,raw=False):
        sqs_c = boto3.client('sqs',
                         aws_access_key_id=session['accessKey'],
                         aws_secret_access_key=session['secretKey'],
                         aws_session_token=session['sessionToken'])
        msgs = []
        while True:
            print("Checking queue {0}".format(session['sqsUrl']))
            r = sqs_c.receive_message(QueueUrl=session['sqsUrl'],
                                  MaxNumberOfMessages=10)
            if 'Messages' not in r:
                break
            for m in r['Messages']:
                init_ctr,msg = m['Body'].split('|',1)
                ctr = pyaes.Counter(initial_value=int(init_ctr))
                aes = pyaes.AESModeOfOperationCTR(base64.b64decode(session['aesKey']),counter=ctr)
                dec_m = aes.decrypt(base64.b64decode(msg))
                print("   Found message: {0}".format(dec_m))
                msgs.append(json.loads(dec_m))
                sqs_c.delete_message(QueueUrl=session['sqsUrl'], ReceiptHandle=m['ReceiptHandle'])
        if raw:
            return sorted(msgs)
        else:
            return sorted([x['msg'] for x in msgs])

"""

    def encrypt_streams(self, key, string, block_size):
        # makes string multiple of 16
        pad = lambda s: s + (block_size - len(s) % block_size) * chr(block_size - len(s) % block_size)
        string = pad(string)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(key, AES.MODE_CBC, iv)
        return base64.urlsafe_b64encode(iv + cipher.encrypt(string))


    def decrypt_streams(self, key, string, block_size):
        unpad = lambda s : s[:-ord(s[len(s)-1:])]
        string = base64.urlsafe_b64decode(string)
        iv = string[:block_size]
        cipher = AES.new(key, AES.MODE_CBC, iv )
        return unpad(cipher.decrypt(string[block_size:]))


key = 'LyfZTeBjLOmjTsoq'
string = 't-QGGHNPkGR2vSXR1J7fYIfUQE7L5RBbmNkUHi9Nh8e2EV1JSe4GNbjypaX8_o92'

print(decrypt(key, string, 16))

key = 'LyfZTeBjLOmjTsoq'
string = 'john.west@mydatahack.com'

print(encrypt(key, string, 16))
"""

if __name__ == '__main__':
    #print(Security.get_key_new())
    #text = "p15d5eh2gahoo6ck2gn6qiocavp"
    #with open("/Users/jianhuang/PycharmProjects/Data/Data/Connect/.pass", "r") as f:
        #line = f.readlines()

    text = 'Zaqwsx567$$'
    key_str = 'LyfZTeBjLOmjTsoq'


    key = key_str.encode('utf-8')
    print(key)
    encrypted_text = Security.Encrypt(key, text)
    print(encrypted_text)
    #print(str(encrypted_text).encode('utf-8'))
    decoded_text = Security.Decrypt(key, encrypted_text)
    print(decoded_text)
    assert(text == Security.Decrypt(key, encrypted_text))
