
import random
import gym
import numpy as np


class PGShowerEnv(gym.Env):
    def __init__(self):
        super(PGShowerEnv, self).__init__()
        self.action_space = gym.spaces.Discrete(3)
        self.observation_space = gym.spaces.Box(low=np.array([0]), high=np.array([100]))
        self.state = 38 + random.randint(-3, 3)
        self.shower_length = 60

    def step(self, action: int):
        self.state += action - 1
        self.shower_length -= 1
        reward = 1 if 37 < self.state <= 39 else -1
        done = True if self.shower_length <= 0 else False
        info = {}
        return self.state, reward, done, info

    def render(self):
        pass

    def reset(self):
        self.state = np.array([38 + random.randint(-3, 3)]).astype(float)
        self.shower_length = 60
        return self.state
