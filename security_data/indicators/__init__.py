import numpy as np
from sklearn import linear_model

DATE_OUTPUT_FORMAT = '%Y%m%d%H%M%S'
REG_MODEL = linear_model.LinearRegression()

BB_MEAN_COL_TEMP = '__bb_mean_col'
BB_STD_COL_TEMP = '__bb_std_dev_col'


class Indicators:

    def __init__(self, df):
        self.df = df

    def sma(self, period=30, name=None, of='close'):
        name = 'sma{0}'.format(period) if name is None else name

        self.df[name] = self.df[of].rolling(period).mean()
        return self

    def bollinger_band(self, period=30, n_std=2, upper_name=None, lower_name=None):

        upper_name = 'BB{0}Upper'.format(n_std) if upper_name is None else upper_name
        lower_name = 'BB{0}Lower'.format(n_std) if lower_name is None else lower_name

        self.df[BB_MEAN_COL_TEMP] = self.df['close'].rolling(period).mean()
        self.df[BB_STD_COL_TEMP] = self.df['close'].rolling(period).std()
        self.df[upper_name] = self.df[BB_MEAN_COL_TEMP] + n_std * self.df[BB_STD_COL_TEMP]
        self.df[lower_name] = self.df[BB_MEAN_COL_TEMP] - n_std * self.df[BB_STD_COL_TEMP]

        del self.df[BB_MEAN_COL_TEMP]
        del self.df[BB_STD_COL_TEMP]
        return self

    def linear_regression(self, period=10, name=None, of='close'):
        name = 'RegLine{0}'.format(period) if name is None else name

        self.df[name] = self.df[of].rolling(period).apply(Indicators._linear_regression_value)
        return self

    def std(self, period=30, name=None, of='close'):
        name = 'std{0}'.format(period) if name is None else name

        self.df[name] = self.df[of].rolling(period).std()
        return self

    def stretch(self, name='stretch'):
        self.df[name] = (self.df['close'] - self.df['avg30']) / self.df['close']

        return self

    def pinch(self, name='pinch'):
        self.df[name] = self.df['std30'] / self.df['close']

        return self

    def z_stretch(self, name='z_stretch', stretch_col='stretch', avg_col='stretch_avg_500', std_col='stretch_std_500'):
        self.df[name] = (self.df[stretch_col] - self.df[avg_col]) / self.df[std_col]

        return self

    def z_pinch(self, name='z_pinch', pinch_col='pinch', avg_col='pinch_avg_500', std_col='pinch_std_500'):
        self.df[name] = (self.df[pinch_col] - self.df[avg_col]) / self.df[std_col]

        return self

    def turn(self, name, col):
        self.df[name] = (self.df[col].shift(2) - self.df[col].shift(1)) / (self.df[col].shift(1) - self.df[col]) < 0

        return self

    def cross_over(self, name, col1, col2):
        self.df[name] = (self.df[col1].shift(1) - self.df[col2].shift(1)) / (self.df[col1] - self.df[col2]) < 0

        return self

    def is_up_river(self, name, avg_col='avg30', std_col='std30'):
        self.df[name] = self.df['close'] > (self.df[avg_col] + self.df[std_col])

        return self

    def is_down_river(self, name, avg_col='avg30', std_col='std30'):
        self.df[name] = self.df['close'] < (self.df[avg_col] - self.df[std_col])

        return self

    def rolling_count(self, name, col_name, count, threshold):
        temp_col = 'roll_count_temp_col'

        self.df[temp_col] = self.df[col_name].map({True: 1, False: 0})
        self.df[name] = self.df[temp_col].rolling(count).sum() >= threshold

        del self.df[temp_col]

        return self

    def is_owl(self, name, cols=['owl_up_move', 'owl_down_move', 'owl_rl10_turn', 'owl_rl30_turn', 'owl_dragon_turn', 'owl_rl10xrl30', 'owl_rl10xdragon'],
               must_true_cols=['owl_up_move', 'owl_down_move'], threshold=4):
        self.df[name] = np.where((self.df[must_true_cols].isin({True}).sum(1) >= 1) & (self.df[cols].isin({True}).sum(1) > threshold), True, False)

        return self

    def is_owl_appear(self, name, col_name):
        self.df[name] = (self.df[col_name].shift(1) != self.df[col_name]) & self.df[col_name]

        return self

    def frogbox(self, upper_name, lower_name, long_period, short_period):
        temp_range_col = 'temp_range_col'
        temp_long_period_std_col = 'temp_long_period_std_col'
        temp_short_period_std_col = 'temp_short_period_std_col'
        temp_min_std_col = 'temp_min_std_col'

        self.df[temp_range_col] = self.df['high'] - self.df['low']
        self.df[temp_long_period_std_col] = self.df[temp_range_col].shift(1).rolling(long_period).std()
        self.df[temp_short_period_std_col] = self.df[temp_range_col].shift(1).rolling(short_period).std()

        self.df[temp_min_std_col] = self.df[[temp_long_period_std_col, temp_short_period_std_col]].min(axis=1)

        self.df[upper_name] = self.df['open'] + self.df[temp_min_std_col]
        self.df[lower_name] = self.df['open'] - self.df[temp_min_std_col]

        del self.df[temp_long_period_std_col]
        del self.df[temp_short_period_std_col]
        del self.df[temp_min_std_col]
        del self.df[temp_range_col]

        return self

    def drop_nan(self):
        self.df = self.df.dropna()
        return self

    def to_dict(self):
        self.df = self.df.reset_index()
        return self.df.to_dict(orient='records')

    @staticmethod
    def _linear_regression_value(data_list):
        data_size = data_list.size
        x_axis = np.array([range(data_list.size)]).T
        REG_MODEL.fit(x_axis, np.array(data_list))
        return REG_MODEL.predict(np.array(data_size - 1))[0]
