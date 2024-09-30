import numpy as np
import pandas as pd
import shap
import xgboost
from matplotlib import pyplot as plt
from pymongo import MongoClient
from sklearn.calibration import LabelEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, explained_variance_score, mean_absolute_error
from sklearn.linear_model import LinearRegression, LogisticRegression, Lasso, Ridge
from sklearn.neighbors import KNeighborsRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.svm import SVR
from sklearn.tree import ExtraTreeRegressor, DecisionTreeRegressor

CLIENT = MongoClient('mongodb://localhost:27017/')
DATABASE = CLIENT['cdl']


def eval(model_name, y_test, y_pred):
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    explained_variance = explained_variance_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    return {
        'model_name': model_name,
        'Mean Squared Error (MSE)': mse,
        'R^2 Score': r2,
        'Explained Variance Score': explained_variance,
        'Mean Absolute Error (MAE)': mae
    }


def shap_run(collection_name, country, x_column, y_column):
    data = pd.DataFrame(DATABASE[collection_name].find({}))
    if country != 'All':
        data = data[data['country'] == country]
    modes = data.mode().iloc[0]
    # 使用众数填充缺失值
    for col in data.columns:
        if pd.isnull(modes[col]):  # 如果众数为空，则不填充该列
            continue
        data[col] = data[col].fillna(modes[col])
    X = data[x_column]
    y = data[y_column]
    label_encoders = {}
    for column in X.select_dtypes(include=['object']).columns:
        label_encoders[column] = LabelEncoder()
        X[column] = label_encoders[column].fit_transform(X[column])
    for column, encoder in label_encoders.items():
        print(f"类别编码映射 ({column}):", dict(zip(encoder.classes_, encoder.transform(encoder.classes_))))
    feature_names = X.columns
    X_train, X_test, y_train, y_test = train_test_split(np.array(X), np.array(y), test_size=0.1, random_state=42)
    print(X_train[0])
    metrics = []
    # model = xgboost.XGBRegressor().fit(X_train, y_train)
    # model_dict = {'xgboost':xgboost.XGBRegressor(),'linear':LinearRegression(),'logistic':LogisticRegression()}
    for model_name in ['xgboost', 'linear', 'knnR','svr','ridge','lasso','mlp','decisionTree','ExtraTreeRegressor','RandomForestRegressor','']:
        if model_name == 'xgboost':
            model = xgboost.XGBRegressor().fit(X_train, y_train)
        elif model_name == 'linear':
            model = LinearRegression().fit(X_train, y_train)
        elif model_name == 'knnR':
            model = KNeighborsRegressor().fit(X_train, y_train)
        elif model_name == 'svr':
            model = SVR().fit(X_train, y_train)
        elif model_name == 'ridge':
            model = Ridge().fit(X_train, y_train)
        elif model_name == 'lasso':
            model = Lasso().fit(X_train, y_train)
        elif model_name == 'mlp':
            model = MLPRegressor().fit(X_train, y_train)
        elif model_name == 'decisionTree':
            model = DecisionTreeRegressor().fit(X_train, y_train)
        elif model_name == 'ExtraTreeRegressor':
            model = ExtraTreeRegressor().fit(X_train, y_train)
        elif model_name == 'RandomForestRegressor':
            model = RandomForestRegressor().fit(X_train, y_train)
        else:
            continue
        y_pred = model.predict(X_test)
        metrics.append(eval(model_name, y_test, y_pred))
    print(metrics)
    pd.DataFrame(metrics).to_csv(f'result/{country}_{y_column}_metrics.csv', index=False)
    # 将 DataFrame 写入 CSV 文件
    # metrics_df.to_csv(f'result/{country}_{y_column}_metrics.csv', index=False)
    #
    # # compute SHAP values
    # explainer = shap.Explainer(model, X_train, feature_names=feature_names)
    # shap_values = explainer(X_train)
    # # 绘制SHAP汇总图
    # # shap.decision_plot(expected_value, shap_values, features_display)
    # shap.plots.beeswarm(shap_values, color_bar=False, show=False, max_display=20)
    # plt.savefig(f'result/{country}_{y_column}_beeswarm.png', dpi=300, bbox_inches='tight')  # 保存图形为图片文件（PNG格式）
    # plt.clf()
    # shap.plots.bar(shap_values, show=False, max_display=20)
    # plt.savefig(f'result/{country}_{y_column}_bar.png', dpi=300, bbox_inches='tight')  # 保存图形为图片文件（PNG格式）
    # plt.clf()
    # for column_name in x_column:
    #     shap.plots.scatter(shap_values[:, column_name], show=False)
    #     plt.savefig(f'result/{country}_{y_column}_scatter_{column_name}.png', dpi=300,
    #                 bbox_inches='tight')  # 保存图形为图片文件（PNG格式）
    # plt.clf()
