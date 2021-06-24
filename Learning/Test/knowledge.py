import pandas as pd
import tensorflow as tf
import matplotlib
import seaborn as sns
###scaler
diabetes = pd.read_csv('pima-indians-diabetes.csv')
cols_to_norm = ['Number_pregnant', 'Glucose_concentration', 'Blood_pressure', 'Triceps', 'Insulin', 'BMI', 'Pedigree']
diabetes[cols_to_norm] = diabetes[cols_to_norm].apply(lambda x: (x - x.min()) / (x.max() - x.min())) # formula to scale numbers between 0 and 1 without using library


### preprocessing

#example1 transform text to numbers - sklearn
from sklearn.preprocessing import LabelEncoder
labelencoder = LabelEncoder()
X[:, 3] = labelencoder.fit_transform(X[:, 3])







###split dataset
x_data = 0 #data
result = 0 #actual result
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x_data, result, test_size=0.33, random_state=101, shuffle=False)

### training
#example1 - tensorflow
input_func = tf.estimator.inputs.pandas_input_fn(x=X_train, y=y_train, batch_size=10, num_epochs=1000, shuffle=True)
model = tf.estimator.LinearClassifier(feature_columns=feat_cols, n_classes=2)
model.train(input_fn=input_func, steps=1000)

### test
#example1 - tensorflow
pred_input_func = tf.estimator.inputes.pandas_input_fn (x=X_test, batch_size=10, num_epochs=1, shuffle=False)
predictions = model.predict(pred_input_func)
list(predictions)

### evaluation
#example1 - tensorflow
eval_input_func = tf.estimator.inputs.pandas.input_fn(x=X_test, y=y_test, batch_size=10, num_epochs=1, shuffle=False)
results = model.evaluate(eval_input_func)
print(results)

# iloc
companies = pd.read_csv('companies.txt')
X = companies.iloc[:, :-1].values #every rows and every columns except the last
y = companies.iloc[:, 4].values # every rows and 4th columns

# visualization

assign_group = tf.feature_column.categorical_column_with_vocabulary_list('Group', ['A', 'B', 'C', 'D'])
import matplotlib.pyplot as plt
#%matplotlib inline

sns.heatmap(companies.corr()) # coorrelation
diabetes['Age'].hist(bins=20) # histogram