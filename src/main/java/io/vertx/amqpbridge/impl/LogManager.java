package io.vertx.amqpbridge.impl;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


public class LogManager {
    private final Logger _logger;

    private final String _prefix;

    LogManager(String prefix, Class<?> clazz) {
      _logger = LoggerFactory.getLogger(clazz);
      _prefix = prefix;
    }

    public boolean isDebugEnabled() {
      return _logger.isInfoEnabled(); //_logger.isDebugEnabled();
    }

    public boolean isInfoEnabled() {
      return _logger.isInfoEnabled();
    }

    public static LogManager get(String prefix, Class<?> clazz) {
      return new LogManager(prefix, clazz);
    }

    public void debug(String format, Object... args) {
      if (_logger.isInfoEnabled()) {
        _logger.info(_prefix.concat(String.format(format, args)));
      }
    }

    public void debug(Throwable e, String format, Object... args) {
      if (_logger.isInfoEnabled()) {
        _logger.info(_prefix.concat(String.format(format, args)), e);
      }
    }

    public void info(String format, Object... args) {
      if (_logger.isInfoEnabled()) {
        _logger.info(_prefix.concat(String.format(format, args)));
      }
    }

    public void info(Throwable e, String format, Object... args) {
      if (_logger.isInfoEnabled()) {
        _logger.info(_prefix.concat(String.format(format, args)), e);
      }
    }

    public void warn(String format, Object... args) {
      _logger.fatal(_prefix.concat(String.format(format, args)));
    }

    public void warn(Throwable e, String format, Object... args) {
      _logger.warn(_prefix.concat(String.format(format, args)), e);
    }

    public void fatal(String format, Object... args) {
      _logger.fatal(_prefix.concat(String.format(format, args)));
    }

    public void fatal(Throwable e, String format, Object... args) {
      _logger.fatal(_prefix.concat(String.format(format, args)), e);
    }
  }