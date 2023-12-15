class Utils:
    """Utils class containing generic methods to be
    used for things like byte conversion"""

    @staticmethod
    def gb_to_mb(func):
        """Decorator method, used to onvert gb to mb"""

        def wrapper(*args, **kwargs):
            return func(*args, **kwargs) * 1024

        return wrapper
