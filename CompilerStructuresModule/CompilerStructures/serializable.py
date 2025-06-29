# AI
from decimal import Decimal


class Serializable:
    def to_dict(self):
        result = {}
        excluded_attributes = []
        
        # Check if the subclass has the 'exclude_attributes' method
        if hasattr(self, 'exclude_attributes') and callable(getattr(self, 'exclude_attributes')):
            excluded_attributes = self.exclude_attributes()
        
        for k, v in self.__dict__.items():
            if k in excluded_attributes:
                continue  # Skip excluded attributes
            
            if isinstance(v, Serializable):
                result[k] = v.to_dict()
            elif isinstance(v, Decimal):
                result[k] = int(v) if v % 1 == 0 else float(v)
            elif isinstance(v, dict):
                # Recursively handle dict values
                result[k] = {key: val.to_dict() if isinstance(val, Serializable) else val for key, val in v.items()}
            elif isinstance(v, list):
                # Recursively handle lists
                result[k] = [item.to_dict() if isinstance(item, Serializable) else item for item in v]
            else:
                result[k] = v
        return result