#!/usr/bin/env bash
set -euo pipefail

LOCATION="${LOCATION:-eastus}"
RESOURCE_GROUP="${RESOURCE_GROUP:-cdp-rt-segmentation-rg}"
NAMESPACE="${NAMESPACE:-cdprtseg$RANDOM}"
EVENTHUB_NAME="${EVENTHUB_NAME:-tealium-events}"
PARTITIONS="${PARTITIONS:-8}"
OWNER="${OWNER:-}"

az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --tags owner="$OWNER" \
  --output none

az eventhubs namespace create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$NAMESPACE" \
  --location "$LOCATION" \
  --sku Standard \
  --capacity 1 \
  --tags owner="$OWNER" \
  --output none

az eventhubs eventhub create \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$NAMESPACE" \
  --name "$EVENTHUB_NAME" \
  --partition-count "$PARTITIONS" \
  --output none

CONNECTION_STRING="$(az eventhubs namespace authorization-rule keys list \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$NAMESPACE" \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString \
  --output tsv)"

cat <<EOF
EVENTHUB_BOOTSTRAP=${NAMESPACE}.servicebus.windows.net:9093
EVENTHUB_NAME=${EVENTHUB_NAME}
EVENTHUB_CONNECTION_STRING=${CONNECTION_STRING};EntityPath=${EVENTHUB_NAME}
RESOURCE_GROUP=${RESOURCE_GROUP}
NAMESPACE=${NAMESPACE}
EOF
