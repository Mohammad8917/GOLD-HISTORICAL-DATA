"""Regression tests for the DST-aware Dukascopy XAU/USD session calendar."""
import unittest

import pandas as pd

import gold_pipeline as pipeline


class SessionCalendarTests(unittest.TestCase):
    def ts(self, value):
        return pd.Timestamp(value, tz="UTC")

    def test_winter_daily_break_and_reopen(self):
        self.assertTrue(pipeline.is_scheduled_trading_hour(self.ts("2026-11-12 21:00")))
        self.assertFalse(pipeline.is_scheduled_trading_hour(self.ts("2026-11-12 22:00")))
        self.assertTrue(pipeline.is_scheduled_trading_hour(self.ts("2026-11-12 23:00")))

    def test_summer_daily_break_tracks_dst(self):
        self.assertTrue(pipeline.is_scheduled_trading_hour(self.ts("2026-07-09 20:00")))
        self.assertFalse(pipeline.is_scheduled_trading_hour(self.ts("2026-07-09 21:00")))
        self.assertTrue(pipeline.is_scheduled_trading_hour(self.ts("2026-07-09 22:00")))

    def test_friday_close_and_weekend(self):
        self.assertTrue(pipeline.is_scheduled_trading_hour(self.ts("2026-11-13 21:00")))
        self.assertFalse(pipeline.is_scheduled_trading_hour(self.ts("2026-11-13 22:00")))
        self.assertFalse(pipeline.is_scheduled_trading_hour(self.ts("2026-11-14 12:00")))
        self.assertFalse(pipeline.is_scheduled_trading_hour(self.ts("2026-11-15 22:00")))
        self.assertTrue(pipeline.is_scheduled_trading_hour(self.ts("2026-11-15 23:00")))

    def test_daily_maintenance_gap_is_scheduled(self):
        self.assertTrue(pipeline.weekend_gap(
            self.ts("2026-11-12 21:59"), self.ts("2026-11-12 23:01")
        ))

    def test_gap_with_missing_open_hour_is_not_hidden_by_break(self):
        self.assertFalse(pipeline.weekend_gap(
            self.ts("2026-11-12 21:59"), self.ts("2026-11-13 00:01")
        ))

    def test_dst_transition_uses_new_york_zone(self):
        # US DST starts 2026-03-08: Sunday 18:00 New York is 22:00 UTC.
        self.assertFalse(pipeline.is_scheduled_trading_hour(self.ts("2026-03-08 21:00")))
        self.assertTrue(pipeline.is_scheduled_trading_hour(self.ts("2026-03-08 22:00")))


if __name__ == "__main__":
    unittest.main()
