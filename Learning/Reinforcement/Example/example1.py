import pandas as pd
from Learning.Reinforcement import pgreinforcement


if __name__ == '__main__':
    df = pd.read_csv('/Users/jianhuang/Stock-Trading-Environment/data/AAPL.csv')
    df = df.sort_values('Date')
    example1 = pgreinforcement.PGLearningReinforcementExt()
    example1.data_preprocessing()
    example1.set_custom_env_param('stocktrading', df)

    print(example1._custom_env_param)

    #example1._process(None, {'entity_name': "custom", 'model_name': "DQN"})
    #example1._process(None, {'entity_name': "custom", 'model_name': "custom"})
    #example1._process(None, {'entity_name': "shower", 'model_name': "PPO"})
    example1._process(None, {'entity_name': "stocktrading", 'model_name': "PPO"})
    #example1._process(None, {'environment': "CartPole-v0", 'entity_name': "CartPole-v0", 'model_name': "DQN"})


