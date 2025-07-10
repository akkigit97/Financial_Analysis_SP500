"""
MLP Classifier for Predicting S&P 500 Price Direction
Using Top 10 Stocks, Macroeconomic Data, and News Sentiment
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
from pymongo import MongoClient
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import accuracy_score, classification_report, log_loss
from mongoDB_setup import connect_mongo


#Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# Function to Fetch Data from MongoDB
def fetch_data():
    """
    Fetches feature-engineered data from MongoDB.
    Returns:
        - Pandas DataFrame containing the dataset.
    """
    db = connect_mongo()
    collection = db["feature_engineering"]

    logging.info("Fetching data from MongoDB...")
    data = pd.DataFrame(list(collection.find()))

    # Drop `_id` column if present
    data.drop(columns=["_id"], errors="ignore", inplace=True)
    return data


# Function to Handle Missing Features
def handle_missing_features(data, features):
    """
    Ensures all required features exist in the dataset.
    Args:
        - data: Pandas DataFrame
        - features: List of required feature column names

    Returns:
        - Updated DataFrame with missing features added.
    """
    for feature in features:
        if feature not in data.columns:
            logging.warning(f"Missing Feature: {feature} -> Creating Column with Default 0")
            data[feature] = 0
    return data


# Function to Split Data into Training and Testing
def split_data(data):
    """
    Splits data into train (before Feb 2024) and test (Feb-Mar 2024).
    Args:
        - data: Pandas DataFrame

    Returns:
        - train_data: Training DataFrame
        - test_data: Testing DataFrame
    """
    test_start = "2024-02-01"
    test_end = "2024-03-31"

    train_data = data[data["Date"] < test_start]
    test_data = data[(data["Date"] >= test_start) & (data["Date"] <= test_end)]

    return train_data, test_data


# Function to Standardize Data
def standardize_data(X_train, X_test):
    """
    Standardizes the dataset using StandardScaler.
    Args:
        - X_train: Training feature set
        - X_test: Testing feature set

    Returns:
        - Scaled X_train and X_test
    """
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)

    return X_train, X_test


# Function to Train MLP Model
def train_mlp(X_train, y_train):
    """
    Trains an MLP classifier with early stopping.
    Args:
        - X_train: Training feature set
        - y_train: Training target values

    Returns:
        - Trained MLPClassifier model
    """
    mlp = MLPClassifier(
        hidden_layer_sizes=(128, 64, 32), activation="tanh", solver="sgd",
        alpha=0.005, max_iter=500, random_state=42, early_stopping=True,
        validation_fraction=0.2, n_iter_no_change=30
    )

    logging.info("Training MLP Classifier with Early Stopping...")
    mlp.fit(X_train, y_train)
    return mlp


# Function to Evaluate the Model
def evaluate_model(model, X_train, y_train, X_test, y_test):
    """
    Evaluates the trained model on both training and test data.
    Args:
        - model: Trained MLP model
        - X_train, y_train: Training feature set and labels
        - X_test, y_test: Testing feature set and labels

    Returns:
        - train_acc: Training accuracy
        - test_acc: Testing accuracy
        - train_loss: Training loss
        - test_loss: Testing loss
    """
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)

    train_acc = accuracy_score(y_train, y_train_pred)
    test_acc = accuracy_score(y_test, y_test_pred)

    train_loss = log_loss(y_train, model.predict_proba(X_train))
    test_loss = log_loss(y_test, model.predict_proba(X_test))

    logging.info(f"\nFinal Train Accuracy: {train_acc:.4f}, Train Loss: {train_loss:.4f}")
    logging.info(f"Final Test Accuracy: {test_acc:.4f}, Test Loss: {test_loss:.4f}")

    return train_acc, test_acc, train_loss, test_loss


# Function to Plot Model Performance
def plot_performance(model, train_acc, test_acc, train_loss, test_loss):
    """
    Plots Training vs Validation Accuracy & Loss and Test Accuracy & Loss.
    """
    val_losses = model.validation_scores_
    train_losses = model.loss_curve_

    # Plot Training & Validation Accuracy
    plt.figure(figsize=(10, 5))
    plt.plot(model.validation_scores_, label="Validation Accuracy", marker=".", color="orange")
    plt.plot(range(len(model.validation_scores_)), [train_acc] * len(model.validation_scores_), label="Training Accuracy", linestyle="dashed", color="blue")
    plt.xlabel("Epochs")
    plt.ylabel("Accuracy")
    plt.title("Training & Validation Accuracy")
    plt.legend()
    plt.grid()
    plt.savefig('training_vs_validation_accuracy.png')

    # Plot Training & Validation Loss
    plt.figure(figsize=(10, 5))
    plt.plot(model.loss_curve_, label="Training Loss", marker=".", color="blue")
    plt.plot(val_losses, label="Validation Loss", marker=".", color="red")
    plt.xlabel("Epochs")
    plt.ylabel("Loss")
    plt.title("Training & Validation Loss")
    plt.legend()
    plt.grid()
    plt.savefig('training_vs_validation_loss.png')

    # Plot Test Accuracy & Loss
    plt.figure(figsize=(10, 5))
    plt.bar(["Test Accuracy", "Test Loss"], [test_acc, test_loss], color=["green", "red"])
    plt.ylabel("Value")
    plt.title("Final Test Accuracy & Loss")
    plt.grid()
    plt.savefig('test_accuracy_and_loss.png')


if __name__ == "__main__":
    logging.info("Starting MLP Model Training Pipeline...")

    # Fetch Data
    data = fetch_data()

    # Define Features & Target
    FEATURES = [
        "Normalized_SP500_Adj_Close", "Normalized_GDP", "Normalized_Inflation",
        "Normalized_Interest_Rate", "Normalized_AAPL_Adj_Close", "Normalized_MSFT_Adj_Close",
        "Normalized_AMZN_Adj_Close", "Normalized_NVDA_Adj_Close", "Normalized_GOOGL_Adj_Close",
        "Normalized_GOOG_Adj_Close", "Normalized_TSLA_Adj_Close", "Normalized_BRK-B_Adj_Close",
        "Normalized_META_Adj_Close", "Normalized_XOM_Adj_Close", "Rolling_Mean_7",
        "Rolling_Mean_30", "Rolling_Volatility_30", "Lag_1", "Lag_3", "Lag_7",
        "Normalized_Avg_News_Sentiment"
    ]
    TARGET = "Price_Direction"

    # Handle Missing Features
    data = handle_missing_features(data, FEATURES)

    # Drop NaN values
    data.dropna(inplace=True)

    # Split Data into Train & Test
    train_data, test_data = split_data(data)
    X_train, y_train = train_data[FEATURES], train_data[TARGET]
    X_test, y_test = test_data[FEATURES], test_data[TARGET]

    # Standardize Data
    X_train, X_test = standardize_data(X_train, X_test)

    # Train MLP Model
    mlp_model = train_mlp(X_train, y_train)

    # Evaluate Model
    train_acc, test_acc, train_loss, test_loss = evaluate_model(mlp_model, X_train, y_train, X_test, y_test)

    # Plot Performance
    plot_performance(mlp_model, train_acc, test_acc, train_loss, test_loss)

    logging.info(" MLP Model Training & Evaluation Completed! ")
