from data_exploration import visualize_data
import logging
from MLP_model import (
    fetch_data, handle_missing_features, split_data,
    standardize_data, train_mlp, evaluate_model, plot_performance
)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    """
    Runs the full pipeline for data exploration, ML model training,
    and evaluation of the S&P 500 prediction model.
    """
    logging.info("Starting the Pipeline for S&P 500 Prediction")

    # Step 1: Fetch Data - already acquired and stored on MongnoDB
    data = fetch_data()

    # Step 2: Perform Data Exploration & Visualization
    logging.info("Running Data Exploration & Visualization")
    visualize_data()
    logging.info("Data Exploration Completed!")

    # Step 3: Preprocessinf and feature engineering - Define Features & Target
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

    # Step 4: Handle Missing Features
    logging.info("Handling Missing Features...")
    data = handle_missing_features(data, FEATURES)
    data.dropna(inplace=True)  # Remove any remaining NaN values
    logging.info("Missing Features Handled!")

    # Step 5: Split Data into Train & Test
    logging.info("Splitting Data into Train & Test Sets...")
    train_data, test_data = split_data(data)
    
    X_train, y_train = train_data[FEATURES], train_data[TARGET]
    X_test, y_test = test_data[FEATURES], test_data[TARGET]
    logging.info("Data Splitting Completed!")

    # Step 6: Standardize Data
    logging.info("Standardizing Data...")
    X_train, X_test = standardize_data(X_train, X_test)
    logging.info("Data Standardization Completed!")

    # Step 7: Train MLP Model
    logging.info("Training MLP Model...")
    mlp_model = train_mlp(X_train, y_train)
    logging.info("Model Training Completed!")

    # Step 8: Evaluate Model Performance
    logging.info("Evaluating Model...")
    train_acc, test_acc, train_loss, test_loss = evaluate_model(
        mlp_model, X_train, y_train, X_test, y_test
    )
    logging.info("Model Evaluation Completed!")

    # Step 9: Plot Performance Metrics
    logging.info("Plotting Model Performance...")
    plot_performance(mlp_model, train_acc, test_acc, train_loss, test_loss)
    logging.info("Performance Visualization Completed!")

    logging.info("Pipeline Execution Completed Successfully!")

# Run the Pipeline from python main.py
if __name__ == "__main__":
    main()