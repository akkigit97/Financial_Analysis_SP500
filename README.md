# DAPS_assignment_24
# SN24242076

This project aims to predict the S&P 500 Index movement using macroeconomic indicators, stock prices, and news sentiment analysis. The model incorporates feature engineering, technical indicators, and machine learning techniques to provide valuable insights into market trends.

How to run the code - Pipeline overview: 
1. Install the required packages using conda. Run 'conda env create -f environment.yml'.
2. Install the required libraries by running `pip install -r requirements.txt` in the terminal.
3. Run the code using `python main.py` in the terminal.
4. The code will generate various plots after acquiring, storing, processing and analyzing the data.
5. The final output will be the predicted S&P 500 Index movement accuracy from the ML model.


Notes: mongoDB_setup.py is used to connect to MongoDB and store the data. MLP_model.py is the ML model used for prediction and is used for training and testing. preprocess_features.py ensures all the acquired data is stored, preprocess and feature engineered.