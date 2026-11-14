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

    def test_clean_ticks_uses_scheduled_hours_before_daily_coverage(self):
        start = pipeline.date(2026, 11, 12)
        end = pipeline.date(2026, 11, 13)
        hours = pipeline.scheduled_hours(
            pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC")
        )
        frame = pd.DataFrame({
            "time": hours + pd.Timedelta(minutes=30),
            "bid": [2000.0] * len(hours),
            "ask": [2000.5] * len(hours),
        })

        cleaned, report = pipeline.clean_ticks(frame, start, end)

        self.assertEqual(len(cleaned), 23)
        self.assertEqual(report["hourly_coverage"]["expected_trading_hours"], 23)
        self.assertEqual(report["hourly_coverage"]["missing_trading_hours"], 0)
        self.assertEqual(report["hourly_coverage"]["low_hour_count"], 0)

    def test_us_holiday_without_dated_provider_notice_is_not_auto_closed(self):
        # A generic US holiday alone is not enough to invent a Dukascopy closure.
        entries = pipeline.calendar_review_entries(
            pipeline.date(2026, 11, 11), pipeline.date(2026, 11, 12)
        )
        self.assertEqual(entries, [])

        frame = pd.DataFrame({
            "time": [self.ts("2026-11-11 23:00")],
            "bid": [2000.0],
            "ask": [2000.5],
        })
        _, report = pipeline.clean_ticks(
            frame, pipeline.date(2026, 11, 11), pipeline.date(2026, 11, 12)
        )
        self.assertEqual(report.get("holiday_calendar_review"), [])
        self.assertFalse(report["holiday_schedule_applied"])

    def test_dst_transition_uses_new_york_zone(self):
        # US DST starts 2026-03-08: Sunday 18:00 New York is 22:00 UTC.
        self.assertFalse(pipeline.is_scheduled_trading_hour(self.ts("2026-03-08 21:00")))
        self.assertTrue(pipeline.is_scheduled_trading_hour(self.ts("2026-03-08 22:00")))


if __name__ == "__main__":
    unittest.main()
