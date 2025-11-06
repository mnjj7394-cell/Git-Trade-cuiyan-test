"""
数据表统一接口基类
定义所有数据表必须实现的统一接口规范
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging


class IDataTable(ABC):
    """数据表统一接口基类"""

    def __init__(self, table_config: Dict[str, Any] = None):
        """统一构造函数签名

        Args:
            table_config: 表配置信息，包含表名、持久化设置等
        """
        self.table_config = table_config or {}
        self.table_name = self.table_config.get('table_name', self.__class__.__name__.lower())
        self._initialized = False
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """设置统一的日志格式"""
        logger = logging.getLogger(f"DataTable.{self.table_name}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    @abstractmethod
    def initialize(self, **kwargs) -> bool:
        """统一初始化方法

        Args:
            **kwargs: 初始化参数，包括adapter, sync_service, event_engine等

        Returns:
            bool: 初始化是否成功
        """
        pass

    @abstractmethod
    def get_table_info(self) -> Dict[str, Any]:
        """获取表信息

        Returns:
            Dict[str, Any]: 表的基本信息
        """
        pass

    @abstractmethod
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """数据验证

        Args:
            data: 待验证的数据

        Returns:
            bool: 数据是否有效
        """
        pass

    @abstractmethod
    def save_data(self, data: Dict[str, Any]) -> bool:
        """保存数据

        Args:
            data: 要保存的数据

        Returns:
            bool: 保存是否成功
        """
        pass

    @abstractmethod
    def query_data(self, conditions: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """查询数据

        Args:
            conditions: 查询条件

        Returns:
            List[Dict[str, Any]]: 查询结果
        """
        pass

    def is_initialized(self) -> bool:
        """检查表是否已初始化"""
        return getattr(self, '_initialized', False)

    def get_config(self) -> Dict[str, Any]:
        """获取表配置"""
        return self.table_config.copy()
