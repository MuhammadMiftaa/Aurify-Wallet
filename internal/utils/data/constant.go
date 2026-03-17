package data

import "time"

var (
	DEVELOPMENT_MODE = "development"
	STAGING_MODE     = "staging"
	PRODUCTION_MODE  = "production"

	OUTBOX_PUBLISH_EXCHANGE     = "refina_microservice"
	OUTBOX_PUBLISH_INTERVAL     = 5 * time.Second
	OUTBOX_PUBLISH_BATCH        = 100
	OUTBOX_PUBLISH_MAX_RETRIES  = 5
	OUTBOX_EVENT_WALLET_CREATED = "wallet.created"
	OUTBOX_EVENT_WALLET_UPDATED = "wallet.updated"
	OUTBOX_EVENT_WALLET_DELETED = "wallet.deleted"

	// Admin exchange and routing keys
	ADMIN_EXCHANGE    = "refina_admin"
	ADMIN_QUEUE       = "refina-admin-wallet-types"
	ADMIN_ROUTING_KEY = "master.wallet_types"

	INITIAL_DEPOSIT_CATEGORY_ID = "00000000-0000-0000-0000-000000000000"
	INITIAL_DEPOSIT_DESC        = "Deposit awal"

	// REQUEST_ID_HEADER is the standard header name used to propagate request IDs.
	REQUEST_ID_HEADER = "X-Request-ID"
	// REQUEST_ID_LOCAL_KEY is the key used to store the request ID in Gin's context locals.
	REQUEST_ID_LOCAL_KEY = "request_id"
)
