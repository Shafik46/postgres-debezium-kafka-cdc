#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONNECTOR_FILE="${1:-ops-connector.json}"
CONNECTOR_PATH="$PROJECT_DIR/connectors/$CONNECTOR_FILE"

if [ ! -f "$CONNECTOR_PATH" ]; then
    echo "Connector file not found: $CONNECTOR_PATH"
    exit 1
fi

CONNECTOR_NAME=$(sed -n 's/.*"name":[[:space:]]*"\([^"]*\)".*/\1/p' "$CONNECTOR_PATH" | head -n 1)

if [ -z "$CONNECTOR_NAME" ]; then
    echo "Could not determine connector name from $CONNECTOR_PATH"
    exit 1
fi

echo "Waiting for Debezium to be ready..."
until curl --silent --fail http://localhost:8083/connectors > /dev/null; do
    echo "Debezium not ready yet, retrying in 5s..."
    sleep 5
done

# delete if already exists
echo "Checking for existing connector..."
STATUS=$(curl --silent -o /dev/null -w "%{http_code}" "http://localhost:8083/connectors/$CONNECTOR_NAME")
if [ "$STATUS" == "200" ]; then
    echo "Connector already exists — deleting first..."
    curl -X DELETE "http://localhost:8083/connectors/$CONNECTOR_NAME"
    sleep 2
fi

echo "Creating connector: $CONNECTOR_NAME"
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @"$CONNECTOR_PATH"

echo ""
echo "Waiting for connector to initialize..."
sleep 10

echo "Connector status:"
curl "http://localhost:8083/connectors/$CONNECTOR_NAME/status"
echo ""
