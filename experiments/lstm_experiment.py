import mlflow

mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("met_deadline_prediction")
mlflow.sklearn.autolog()

with mlflow.start_run():

    mlflow.log_param("model", "LSTM")
    mlflow.log_param("sequence_length", 30)

    accuracy = 0.81

    mlflow.log_metric("accuracy", accuracy)