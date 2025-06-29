"""Logging configuration for US Accidents Lakehouse project."""

import logging
import logging.handlers
import os
from typing import Optional
from datetime import datetime


class Logger:
    """Logger utility class for structured logging."""
    
    _loggers = {}
    
    @classmethod
    def setup_logger(cls, name: str, level: str = "INFO") -> logging.Logger:
        """Set up a logger with the specified name and level.
        
        Args:
            name: Logger name (typically module name)
            level: Logging level (DEBUG, INFO, WARNING, ERROR)
            
        Returns:
            Configured logger instance
        """
        if name in cls._loggers:
            return cls._loggers[name]
        
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()))
        
        if not logger.handlers:
            cls._configure_handlers(logger)
        
        cls._loggers[name] = logger
        return logger
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get an existing logger or create a new one with INFO level.
        
        Args:
            name: Logger name
            
        Returns:
            Logger instance
        """
        if name not in cls._loggers:
            return cls.setup_logger(name, "INFO")
        return cls._loggers[name]
    
    @classmethod
    def _configure_handlers(cls, logger: logging.Logger) -> None:
        """Configure console and file handlers for the logger.
        
        Args:
            logger: Logger instance to configure
        """
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        log_file = os.path.join(log_dir, f"lakehouse_{datetime.now().strftime('%Y%m%d')}.log")
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """Convenience function to get a logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return Logger.get_logger(name)


def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Convenience function to set up a logger.
    
    Args:
        name: Logger name
        level: Logging level
        
    Returns:
        Configured logger instance
    """
    return Logger.setup_logger(name, level)