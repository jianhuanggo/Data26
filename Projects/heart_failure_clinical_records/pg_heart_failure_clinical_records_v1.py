import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.model_selection import RandomizedSearchCV, GridSearchCV
from sklearn.metrics import confusion_matrix,classification_report
from sklearn.metrics import precision_score,recall_score,f1_score
from sklearn.metrics import plot_roc_curve


#Models
import xgboost as xgb
import lightgbm as lgb
from sklearn.svm import SVC
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from imblearn.over_sampling import RandomOverSampler
from joblib import dump, load

#data = pd.read_csv("https://raw.githubusercontent.com/sunnymoon-sultan/Heart_failure_classifier/main/heart.csv",error_bad_lines = False)


def get_input() -> pd.DataFrame:
    return pd.read_csv("heart_failure_clinical_records_dataset.csv", error_bad_lines = False)


def data_info(data: pd.DataFrame):
    print(data.head())
    print(data.info())
    print(f"the size of the data is: {data.size}. and the shape of the dataset is: {data.shape}")
    print(data.columns)
    print(data.nunique())


def pre_processing(data: pd.DataFrame):
    from imblearn.over_sampling import RandomOverSampler
    ros = RandomOverSampler(random_state=42)
    ##we have to define the x(feature variable) and the y(target variable)
    x = data.iloc[:, :-1]
    y = data.iloc[:, -1]
    # fitting the predictor and the target variable
    x_ros, y_ros = ros.fit_resample(x, y)
    y_ros.value_counts().plot(kind="bar", color=["salmon", "blue"], figsize=(10, 6));


def modeling(X_train, X_test, y_train, y_test):
    _models = {"Logistic Regression": LogisticRegression(),
               "KNN": KNeighborsClassifier(),
               "Random Forest": RandomForestClassifier(),
               "SVC": SVC(),
               "Gradient_boosting": GradientBoostingClassifier(),
               "DecissionTree": DecisionTreeClassifier(),
               "lightgbm": lgb.LGBMClassifier(),
               "Xg boost": xgb.XGBClassifier()}
    np.random.seed(42)
    model_scores = {}
    for name,model in _models.items():
        model.fit(X_train, y_train)
        model_scores[name] = model.score(X_test, y_test)
    return model_scores


if __name__ == '__main__':
    data = get_input()
    #data_info(data)
    pre_processing(data)
    #print(data.columns)
    #print(data.time)
    np.random.seed(42)
    X_train, X_test, y_train, y_test = train_test_split(x_ros, y_ros, test_size=0.2)
    x_t, x_te, y_t, y_te = train_test_split(x, y, test_size=0.2)



#the function!

#splitting the data into test and tarin sets!
np.random.seed(42)
x_train, x_test, y_train, y_test = train_test_split(x_ros, y_ros, test_size=0.2)
x_t,x_te,y_t,y_te = train_test_split(x,y,test_size = 0.2)