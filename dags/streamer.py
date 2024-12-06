import pandas as pd
import os
import json

class JsonStreamerPandas:
    def __init__(self, file_path):
        self.file_path = file_path
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist.")
        
        self.data = pd.read_json(file_path, lines=True)  # Load the CSV into a DataFrame
        self.current_index = 0  # Initialize the current row index
        

    def get_next_row(self):
        # Check if there are rows left to return
        if self.current_index < len(self.data):
            row = self.data.iloc[self.current_index].to_dict(),  # Get the current row
            self.current_index += 1  # Move to the next row
            return row
        else:
            # Return None if no more rows are available
            return None