from warehouse.logging_config import configure_logging
from warehouse.schema_registry import register_warehouse_schema
from warehouse.settings import get_settings


def main() -> None:
    configure_logging()
    schema_id = register_warehouse_schema(get_settings())
    print(f"registered warehouse schema id={schema_id}")


if __name__ == "__main__":
    main()
