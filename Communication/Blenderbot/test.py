import os
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

os.environ['KMP_DUPLICATE_LIB_OK'] = 'TRUE'

tokenizer = AutoTokenizer.from_pretrained("facebook/blenderbot-3B")

model = AutoModelForSeq2SeqLM.from_pretrained("facebook/blenderbot-3B")


utterance = "My name is AAA I like coding"

inputs = tokenizer(utterance, return_tensors="pt")
print(inputs)

res = model.generate(**inputs)
print(res[0])

tokenizer.decode(inputs["input_ids"][0])
tokenizer.decode(res[0])


