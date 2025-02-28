from abc import ABC, abstractmethod
import pandas as pd

class Predictor(ABC):
    @abstractmethod
    def predict(self):
        pass

class BasicPredictor(Predictor):
    def __init__(self, input_data_df):
        self.input_data_df = input_data_df

    def predict(self):
        n = []
        
        n.append(self.input_data_df.iloc[-1])
        
        second_hightest_value = self.input_data_df.sort_values(
            ascending=False
        ).iloc[1]
        n.append(second_hightest_value)
        
        difference = n[1] - n[0] 
        n.append(n[1] + (difference / 2))
        
        difference = n[2] - n[1] 
        n.append(n[2] + (difference / 4))
        
        n.pop(0)

        # output_df = pd.DataFrame(
        #     {"Values": n}, 
        #     index=None
        # )
        # return output_df
        return n
 
class NumPyPredictor(Predictor):
    def __init__(self, x, y, degree=1):
        self.x = x
        self.y = y
        self.degree = degree

    def predict(self):
        coefficients = np.polyfit(
            self.x, self.y, self.degree
        )
        
        p = np.poly1d(coefficients)

        plt.plot(x, p(x))
        return n