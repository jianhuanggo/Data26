import platform;
print(platform.mac_ver())


import torch
import os
from torch import autocast
from diffusers import StableDiffusionPipeline
from matplotlib import pyplot as plt

os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'
model_id = "CompVis/stable-diffusion-v1-4"
#device = "cuda"
device = "mps"


pipe = StableDiffusionPipeline.from_pretrained(model_id, use_auth_token=True)
pipe = pipe.to(device)

prompt = "a photo of an astronaut riding a horse on mars"
with autocast("cuda"):
    image = pipe(prompt, guidance_scale=7.5)["sample"][0]

#image.save("astronaut_rides_horse.png")
plt.figure(figsize=(10, 10))
plt.imshow(image)
plt.show()
