class Utils:
    """Utils class containing generic methods to be
    used for things like byte conversion"""

    @staticmethod
    def gb_to_mb(memory: int) -> int:
        """Convert gb to mb"""
        return memory * 1024

    @staticmethod
    def mb_to_gb(memory: int) -> int:
        """Convert mb to gb"""
        return memory // 1024
