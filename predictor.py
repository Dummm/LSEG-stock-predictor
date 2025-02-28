from apc import ABC, abstractmethod

class Predictor(ABC):
    @abstractmethod
    def predict():
        pass

class BasicPredictor(Predictor):
    def __init__(input_data_df):
        self.input_data_df = input_data_df

    def predict():
        pass