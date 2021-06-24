#pip install transformers

from transformers import AutoTokenizer, AutoModelForCausalLM
tokenizer = AutoTokenizer.from_pretrained("distilgpt2")
model = AutoModelForCausalLM.from_pretrained("distilgpt2", output_hidden_states=True)

text = "The Shawshank"

# Tokenize the input string
input = tokenizer.encode(text, return_tensors="pt")

# Run the model
output = model.generate(input, max_length=5, do_sample=False)

# Print the output
print('\n', tokenizer.decode(output[0]))

text = "The Shawshank"
input = tokenizer(text, return_tensors="pt")['input_ids']
print(input)

tokenizer.convert_ids_to_tokens(input[0])

# This is the embedding matrix of the model
model.transformer.wte # Dimensions are: (Number of tokens in vocabulary, dimension of model)

# Get the embedding vector of token # 464 ('The')
model.transformer.wte(torch.tensor(464))

text = "The chicken didn't cross the road because it was"

# Tokenize the input string
input = tokenizer.encode(text, return_tensors="pt")

# Run the model
output = model.generate(input, max_length=20, do_sample=True)

# Print the output
print('\n', tokenizer.decode(output[0]))