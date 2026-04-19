#!/bin/bash

CONNECTOR_NAME="${1:-ops-lifecycle-connector}"

echo "Deleting connector: $CONNECTOR_NAME"
curl -X DELETE "http://localhost:8083/connectors/$CONNECTOR_NAME"
echo "Done."
