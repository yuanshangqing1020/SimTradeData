#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clean Mootdx data from DuckDB database while preserving BaoStock data

This script removes data downloaded from Mootdx (Phase 1) while keeping
BaoStock data (Phase 2).
"""

import argparse
from pathlib import Path
from simtradedata.writers.duckdb_writer import DEFAULT_DB_PATH, DuckDBWriter

def clean_mootdx_data(db_path: str = DEFAULT_DB_PATH) -> None:
    """Clean Mootdx data from database while preserving BaoStock data."""
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"Database not found: {db_path}")
        return

    print(f"Cleaning Mootdx data from: {db_path}")
    print("=" * 70)
    print("Tables to preserve (BaoStock):")
    print("  - valuation")
    print("  - index_constituents")
    print("  - stock_status")
    print("  - version_info (metadata)")
    print("\nTables to clean (Mootdx):")
    print("  - stocks")
    print("  - exrights")
    print("  - fundamentals")
    print("  - benchmark")
    print("  - trade_days")
    print("  - stock_metadata")
    print("  - stock_pool")
    print("  - sampling_progress")
    print("  - fundamentals_progress")
    print("  - money_flow")
    print("  - lhb")
    print("  - margin_trading")
    print("  - _bonus_ps_corrections")
    print("=" * 70)

    writer = DuckDBWriter(db_path=db_path)
    try:
        # Tables to clean (Mootdx data)
        mootdx_tables = [
            "stocks",
            "exrights",
            "fundamentals",
            "benchmark",
            "trade_days",
            "stock_metadata",
            "stock_pool",
            "sampling_progress",
            "fundamentals_progress",
            "money_flow",
            "lhb",
            "margin_trading",
            "_bonus_ps_corrections"
        ]

        # Begin transaction
        writer.begin()

        try:
            # Clean each Mootdx table
            for table in mootdx_tables:
                try:
                    # Check if table exists
                    result = writer.conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'").fetchone()
                    if result:
                        # Get row count before deletion
                        count_result = writer.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                        count = count_result[0] if count_result else 0
                        
                        # Drop the table completely
                        writer.conn.execute(f"DROP TABLE IF EXISTS {table}")
                        print(f"  Dropped table {table} (contained {count:,} rows)")
                except Exception as e:
                    print(f"  Error cleaning {table}: {e}")

            # Commit transaction
            writer.commit()
            
            # Run VACUUM to reclaim disk space
            try:
                writer.conn.execute("VACUUM;")
                print("\n✅ VACUUM completed - disk space reclaimed!")
            except Exception as e:
                print(f"\n⚠️  VACUUM failed: {e}")
            
            print("\n✅ Mootdx tables dropped successfully!")
            print("\nBaoStock data (valuation, index_constituents, stock_status) has been preserved.")

        except Exception as e:
            writer.rollback()
            print(f"\n❌ Error during cleaning: {e}")

    finally:
        writer.close()

def main():
    parser = argparse.ArgumentParser(
        description="Clean Mootdx data from DuckDB database while preserving BaoStock data"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"Path to DuckDB database (default: {DEFAULT_DB_PATH})"
    )
    args = parser.parse_args()

    clean_mootdx_data(args.db_path)

if __name__ == "__main__":
    main()
