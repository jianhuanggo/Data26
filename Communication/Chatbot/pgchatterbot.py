"""
Only work with python 3.7 with modification

package need to be installed:

pip install chatterbot
pip install chatterbot_corpus
pip3 install nltk
pip install spacy
### english language pack for spacy
python -m spacy download en_core_web_sm

Issue: spacy 3.0 doesn't not support shorten name for language pack

Solution: create a mapping between shorten name and corresponding name

class PosLemmaTagger(object):

    def __init__(self, language=None):

        ### modify to make it compatible with spacy 3.0
        spacy_lang_module_map = {'en': 'en_core_web_sm'}  ## <-- code added here
        self.language = language or languages.ENG

        print(languages.ENG.ISO_639_1.lower())

        self.punctuation_table = str.maketrans(dict.fromkeys(string.punctuation))

        ### modify to make it compatible with spacy 3.0
        self.nlp = spacy.load(spacy_lang_module_map[self.language.ISO_639_1.lower()]) ## <-- code modified here

predefined corpus:

'training_data': [
         'chatterbot.corpus.english.greeting',
         'chatterbot.corpus.custom.myown',
         'chatterbot.corpus.swedish.food'
    ]
"""

import os
import re
from chatterbot import ChatBot
from chatterbot.trainers import ListTrainer
from chatterbot import corpus
from Data.Utils import pgdirectory
from Data.Utils import pgyaml as pgyl
from Communication import pgcombase
import functools


class PGChatterbot(pgcombase.PGCommBase):

    def __init__(self):
        super().__init__()
        self._config.parameters = pgyl.yaml_load(yaml_filename=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml")
        self._config.parameters['BUILTIN_CORPUS_DATA_EN'] = corpus.get_file_path('chatterbot.corpus.english')

        print(self._config.parameters)

        self._bot = ChatBot(
            'Buddy',
            storage_adapter='chatterbot.storage.SQLStorageAdapter',
            database_uri='sqlite:///database.sqlite3',
            logic_adapters=[
                'chatterbot.logic.BestMatch',
                'chatterbot.logic.TimeLogicAdapter']
        )

    def train(self, corpus_filename):
        # Inport ListTrainer
        trainer = ListTrainer(self._bot)
        trainer.train(corpus_filename)

        """
        trainer.train([
            'Hi',
            'Hello',
            'I need your assistance regarding my order',
            'Please, Provide me with your order id',
            'I have a complaint.',
            'Please elaborate, your concern',
            'How long it will take to receive an order ?',
            'An order takes 3-5 Business days to get delivered.',
            'Okay Thanks',
            'No Problem! Have a Good Day!'
        ])
        """

    def list_corpus_category(self):
        corpus_data = pgdirectory.files_in_dir(self._config.parameters['BUILTIN_CORPUS_DATA_EN']) + \
                      pgdirectory.files_in_dir(self._config.parameters['CORPUS_HOME_DIRECTORY'])
        return list(map(lambda x: x.split('.')[0], corpus_data))

    def load_corpus_filename(self, category):
        corpus_category = self.list_corpus_category()
        if category not in corpus_category:
            print(f"category {category} is not available\nHere is a list of available category: {corpus_category}")
            raise
        return os.path.join(corpus.get_file_path('chatterbot.corpus.english'), f"{category}.yml")

    @functools.cached_property
    def bot(self):
        return self._bot


if __name__ == '__main__':

    #print(os.path.join(corpus.get_file_path('chatterbot.corpus.english'), 'conversations.yml'))


    mybot = PGChatterbot()
    print(mybot.load_corpus_filename('greetings'))

    mybot.train(mybot.load_corpus_filename('greetings'))
    exit(0)
    name = input("Enter Your Name: ")
    print("Welcome to the Bot Service! Let me know how can I help you?")
    while True:
        request = input(name + ':')
        if request == 'Bye' or request == 'bye':
            print('Bot: Bye')
            break
        else:
            response = mybot.bot.get_response(request)
            print('Bot:', response)
