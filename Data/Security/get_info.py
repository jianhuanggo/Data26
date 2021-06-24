import sys
import pyaes
import logging
import argparse


_version_ = 0.5

def Decrypt(key, text):
    aes = pyaes.AESModeOfOperationCTR(key)
    return aes.decrypt(text).decode('utf-8')


def Encrypt(key, text):
    aes = pyaes.AESModeOfOperationCTR(key)
    return aes.encrypt(text)


def get_parser():
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='Daemon Framework - start daemon script')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('-k', '--key', action='store', dest='key_store', required=True, help='Key for decryption')
        argparser.add_argument('-f', '--filename', action='store', dest='file_name', required=True,
                               help='file to be decrypted')

        args = argparser.parse_args()

    except Exception as err:
        logging.critical("Error creating/parsing arguments:\n%s" % str(err))
        sys.exit(100)

    return args


if __name__ == '__main__':

    """
    args = get_parser()
    with open(args.file_name, 'rb') as f:
        encrypted_text = f.read()

    decoded_text = Decrypt(args.key_store, encrypted_text)
    print(decoded_text)
    """

    text = 'Zaqwsx567$$'
    key_str = 'LyfZTeBjLOmjTsoq'

    key = key_str.encode('utf-8')
    print(key)
    encrypted_text = Encrypt(key, text)
    print(encrypted_text)

    with open('LyfZTeBjLOmjTsoq.txt', 'wb') as f:
        f.write(encrypted_text)

    with open('LyfZTeBjLOmjTsoq.txt', 'rb') as f:
        encrypted_text = f.read()

    decoded_text = Decrypt(key, encrypted_text)
    print(decoded_text)
    assert(text == Decrypt(key, encrypted_text))
