import argparse
from pprint import pprint

from cassandra.query import SimpleStatement

from warehouse.cassandra_client import connect_with_retry
from warehouse.cassandra_migrations import apply_migrations
from warehouse.settings import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Query Cassandra inventory tables.")
    parser.add_argument("--product-id")
    parser.add_argument("--zone-id")
    args = parser.parse_args()

    settings = get_settings()
    cluster, session = connect_with_retry(settings)
    try:
        apply_migrations(session, settings)
        if args.product_id and args.zone_id:
            pprint(
                list(
                    session.execute(
                        SimpleStatement("""
                            SELECT *
                            FROM inventory_by_product_zone
                            WHERE product_id = %s AND zone_id = %s
                            """),
                        (args.product_id, args.zone_id),
                    )
                )
            )
        if args.product_id:
            pprint(
                list(
                    session.execute(
                        SimpleStatement("SELECT * FROM inventory_by_product WHERE product_id = %s"),
                        (args.product_id,),
                    )
                )
            )
            pprint(
                list(
                    session.execute(
                        SimpleStatement(
                            "SELECT * FROM inventory_by_product_zone WHERE product_id = %s"
                        ),
                        (args.product_id,),
                    )
                )
            )
        if args.zone_id:
            pprint(
                list(
                    session.execute(
                        SimpleStatement("SELECT * FROM inventory_by_zone WHERE zone_id = %s"),
                        (args.zone_id,),
                    )
                )
            )
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
