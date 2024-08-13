

class Markout_model:
    def __init__(self, df, markout_distance_array):
        self.price_data = df
        self.tickers = tickers # list of tickers
        self.markout_distance_array = markout_distance_array # list of markout distances
        self.fair_value_model = fair_value_model # fair value model

    def calculate_markout(df,self):
        # takes as input a dataframe with token in token out timestamp and amount in amount out
        # returns a dataframe with those fields and markout in bps for each markout distance in markout_distance_array

    def markout_summary(df,self):
        # takes as input a dataframe with token in token out timestamp and amount in amount out
        # returns a dataframe with those fields and markout in bps for each markout distance in markout_distance_array

        # Example of what the summary markout model might look like:
        #
        # | Markout Distance | Mean Markout (bps) | Median Markout (bps) | Std Dev (bps) | Min (bps) | Max (bps) | Count |
        # |-------------------|---------------------|----------------------|----------------|-----------|-----------|-------|
        # | 1 minute          | 5.2                 | 4.8                  | 2.1            | -1.5      | 15.3      | 1000  |
        # | 5 minutes         | 7.8                 | 7.2                  | 3.5            | -2.8      | 22.1      | 1000  |
        # | 15 minutes        | 10.5                | 9.7                  | 4.9            | -4.2      | 31.6      | 1000  |
        # | 1 hour            | 15.3                | 14.1                 | 7.2            | -8.7      | 45.9      | 1000  |
        # | 1 day             | 28.6                | 26.4                 | 12.8           | -15.3     | 82.7      | 1000  |
        
        # Actual implementation would involve calculating these statistics
        # based on the markout values for each distance in markout_distance_array
        
        summary = pd.DataFrame(columns=['Markout Distance', 'Mean Markout (bps)', 'Median Markout (bps)', 
                                        'Std Dev (bps)', 'Min (bps)', 'Max (bps)', 'Count'])
        
        for distance in self.markout_distance_array:
            markout_col = f'markout_{distance}'
            summary = summary.append({
                'Markout Distance': distance,
                'Mean Markout (bps)': df[markout_col].mean(),
                'Median Markout (bps)': df[markout_col].median(),
                'Std Dev (bps)': df[markout_col].std(),
                'Min (bps)': df[markout_col].min(),
                'Max (bps)': df[markout_col].max(),
                'Count': df[markout_col].count()
            }, ignore_index=True)
        
        return summary

# a fair value model takes as input a ticker and a time and returns a fair value
# it can be a simple model or a complex model
# it can be a model that uses historical data to predict the future
# it can be a model that uses a neural network to predict the future
# it can be a model that uses a fundamental analysis to predict the future

class Fair_value_model:
    def __init__(self, df, fair_value_distance_array):
        self.price_data = df
        self.fair_value_distance_array = fair_value_distance_array