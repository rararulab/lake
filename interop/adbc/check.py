"""Black-box checks using Apache Arrow's official ADBC Flight SQL driver."""

from __future__ import annotations

import argparse
import sys

import adbc_driver_manager
import pyarrow as pa
from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import connect


def connect_to(uri: str, token: str | None):
    options: dict[str, str] = {}
    if token is not None:
        options[DatabaseOptions.AUTHORIZATION_HEADER.value] = f"Bearer {token}"
    return connect(uri, db_kwargs=options, autocommit=True)


def query(uri: str, token: str | None) -> None:
    with connect_to(uri, token) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT value FROM lake.interop.rows ORDER BY value")
            reader = cursor.fetch_record_batch()
            batches = list(reader)

    assert len(batches) > 1, f"expected multiple Arrow batches, got {len(batches)}"
    table = pa.Table.from_batches(batches)
    assert table.schema == pa.schema([pa.field("value", pa.int64(), nullable=False)])
    assert table.num_rows == 20_000
    assert table.column("value")[0].as_py() == 1
    assert table.column("value")[-1].as_py() == 20_000


def reject_write(uri: str) -> None:
    try:
        with connect_to(uri, None) as connection:
            with connection.cursor() as cursor:
                cursor.execute("INSERT INTO lake.robot.samples VALUES (1)")
    except adbc_driver_manager.Error as error:
        message = str(error)
        assert "DML not supported" in message, message
        return
    raise AssertionError("Lake accepted public DML through ADBC")


def expect_auth_failure(uri: str, token: str | None) -> None:
    try:
        query(uri, token)
    except adbc_driver_manager.Error as error:
        message = str(error).lower()
        assert "unauthenticated" in message or "authentication" in message, message
        return
    raise AssertionError("Lake accepted a missing or incorrect bearer credential")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument(
        "--mode", choices=("query", "reject-write", "expect-auth-failure"), required=True
    )
    parser.add_argument("--token")
    args = parser.parse_args()

    if args.mode == "query":
        query(args.uri, args.token)
    elif args.mode == "reject-write":
        reject_write(args.uri)
    else:
        expect_auth_failure(args.uri, args.token)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"ADBC interoperability check failed: {error}", file=sys.stderr)
        raise
