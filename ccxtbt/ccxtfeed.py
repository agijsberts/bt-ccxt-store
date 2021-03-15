#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015, 2016, 2017 Daniel Rodriguez
# Copyright (C) 2017 Ed Bartosh
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time
import logging

from collections import deque
from datetime import datetime

import backtrader as bt
from backtrader.feed import DataBase
from backtrader.utils.py3 import with_metaclass

from .ccxtstore import CCXTStore

_log = logging.getLogger(__name__)

# NOTE:
# the partial candle implementation is crap. It'd be better if we do something
# like "subcandles", which would be assigning a value t_sub to each (partial) 
# candle. t_sub would be incremented at predefined intervals (e.g., 10s, 20s, 
# 30s, and so on). The current timestamp would then simple be (t, t_sub). A full
# candle would be (t, inf) or (t, granularity).
# This would allow us to easily check if we want to fetch a new candle. The 
# downside is that we need the local computer time to be accurate to determine
# the age of the candle, since the server time is not propagated by ccxt (some
# exchanges may not return it; bybit does)

class MetaCCXTFeed(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaCCXTFeed, cls).__init__(name, bases, dct)

        # Register with the store
        CCXTStore.DataCls = cls

from autologging import traced
@traced
class CCXTFeed(with_metaclass(MetaCCXTFeed, DataBase)):
    """
    CryptoCurrency eXchange Trading Library Data Feed.
    Params:
      - ``historical`` (default: ``False``)
        If set to ``True`` the data feed will stop after doing the first
        download of data.
        The standard data feed parameters ``fromdate`` and ``todate`` will be
        used as reference.
      - ``backfill_start`` (default: ``True``)
        Perform backfilling at the start. The maximum possible historical data
        will be fetched in a single request.

    Changes From Ed's package

        - Added option to send some additional fetch_ohlcv_params. Some exchanges (e.g Bitmex)
          support sending some additional fetch parameters.
        - Added drop_newest option to avoid loading incomplete candles where exchanges
          do not support sending ohlcv params to prevent returning partial data

    """

    params = (
#        ('historical', False),  # only historical download
#        ('backfill_start', False),  # do backfilling at the start
        ('fetch_ohlcv_params', {}),
        ('partial_candles', False),
        ('limit', 200),
#        ('minimum_candle_age', 0), # discard running candles younger than this many seconds 
#        ('drop_newest', False)
    )

    _store = CCXTStore

    # States for the Finite State Machine in _load
    _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(3)

    # def __init__(self, exchange, symbol, ohlcv_limit=None, config={}, retries=5):
    def __init__(self, **kwargs):
        self.store = self._store(**kwargs)
        self._data = deque()  # data queue for price data
        self._last_id = ''  # last processed trade id for ohlcv
        self._last_ts = 0  # last processed timestamp for ohlcv (in seconds)

    def start(self):
        DataBase.start(self)

        self.granularity = self.store.get_granularity(self._timeframe, self._compression)

        if self.p.todate is not None and self.p.fromdate is None:
            raise ValueError('requested to-date without from-date')

        if self.p.fromdate:
            self._state = self._ST_HISTORBACK
            # NOTE: substracting one second is an ugly workaround to ensure
            # that the fromdate is included. we can probably avoid this workaround
            # with the (t, t_sub) tuples described above.
            self._last_ts = int((self.p.fromdate - datetime(1970, 1, 1)).total_seconds()) - 1
            self.put_notification(self.DELAYED)

        else:
            if self.p.partial_candles:
                raise NotImplementedError('partial candles are not yet supported')

            if self.p.limit < 3:
                raise ValueError('limit should be set at least to 3 to work well')

            self._state = self._ST_LIVE
            self.put_notification(self.LIVE)


    def _load(self):
        if self._state == self._ST_OVER:
            return False

        # historical data at ticks granularity is not supported 
        if self._timeframe == bt.TimeFrame.Ticks:
            return self._load_ticks()

        if len(self._data) == 0:
            self._fetch_ohlcv()

        ohlcv = self._load_ohlcv()
        _log.debug('loaded ohlcv %s', ohlcv)

        # we got a candle
        if ohlcv:
            # quit if we went beyond a to-date
            if self.p.todate is not None and self.lines.datetime[0] >= bt.date2num(self.p.todate):
                _log.info('arrived at the end date %s of the feed', bt.num2date(self.lines.datetime[0]))

                self.put_notification(self.DISCONNECTED)
                self._state = self._ST_OVER
                return False  # end of historical

            # if not, go ahead and return it
            return ohlcv

        else:
            # quit if we had a to-date
            if self.p.todate is not None:
                _log.info('arrived at the end of the feed')

                self.put_notification(self.DISCONNECTED)
                self._state = self._ST_OVER
                return False  # end of historical


            # no new data, so if we were backfilling it means we switch to live
            else:
                _log.info('switching from historical to live data')

                self._state = self._ST_LIVE
                self.put_notification(self.LIVE)
                                                                                                    

    def _fetch_ohlcv(self):
        fetch = True

        # check minimum_candle_age
#        if time.time() < self._last_ts + self.p.minimum_candle_age:
#            fetch = False

        if fetch:
            _log.debug('fetching symbol %s at granularity %s starting from %s (%s)', 
                      self.p.dataname, self.granularity, self._last_ts, datetime.utcfromtimestamp(self._last_ts))

            since = None
            if self._last_ts > 0:
                since = self._last_ts * 1000

            batch = self.store.fetch_ohlcv(self.p.dataname, timeframe=self.granularity,
                                           since=since, limit=self.p.limit)

            batch = sorted(batch)

            if self._state == self._ST_LIVE and not self.p.partial_candles:
                _log.debug('dropping most recent partial candle')
                batch = batch[:-1]

            for ohlcv in batch:
                if None in ohlcv:
                    continue

                tstamp = ohlcv[0] // 1000
                if tstamp > self._last_ts:
                    _log.debug('add ohlcv %s (%s) to deque', ohlcv, datetime.utcfromtimestamp(tstamp))
                    self._data.append(ohlcv)
                    self._last_ts = tstamp


    def _load_ticks(self):
        if self._last_id is None:
            # first time get the latest trade only
            trades = [self.store.fetch_trades(self.p.dataname)[-1]]
        else:
            trades = self.store.fetch_trades(self.p.dataname)

        for trade in trades:
            trade_id = trade['id']

            if trade_id > self._last_id:
                trade_time = datetime.strptime(trade['datetime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                self._data.append((trade_time, float(trade['price']), float(trade['amount'])))
                self._last_id = trade_id

        try:
            trade = self._data.popleft()
        except IndexError:
            return None  # no data in the queue

        trade_time, price, size = trade

        self.lines.datetime[0] = bt.date2num(trade_time)
        self.lines.open[0] = price
        self.lines.high[0] = price
        self.lines.low[0] = price
        self.lines.close[0] = price
        self.lines.volume[0] = size

        return True

    def _load_ohlcv(self):
        try:
            ohlcv = self._data.popleft()
        except IndexError:
            return None  # no data in the queue

        tstamp, open_, high, low, close, volume = ohlcv

        dtime = datetime.utcfromtimestamp(tstamp // 1000)

        self.lines.datetime[0] = bt.date2num(dtime)
        self.lines.open[0] = open_
        self.lines.high[0] = high
        self.lines.low[0] = low
        self.lines.close[0] = close
        self.lines.volume[0] = volume

        _log.info('loading candle from %s - open: %s, high: %s, low: %s, close: %s, volume: %s', 
                   dtime, open_, high, low, close, volume)

        return True

    def haslivedata(self):
        return self._state == self._ST_LIVE and len(self._data) > 0

    def islive(self):
        '''Returns ``True`` to notify ``Cerebro`` that preloading and runonce
        should be deactivated'''
        return self.p.todate is None

