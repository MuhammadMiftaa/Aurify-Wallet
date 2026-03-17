package data

// Service field logging constants
const (
	MainService       = "main"
	EnvService        = "env"
	DatabaseService   = "database"
	RabbitmqService   = "rabbitmq"
	GRPCClientService = "grpc_client"
	GRPCServerService = "grpc_server"
	HTTPServerService = "http_server"
	OutboxService     = "outbox"
	WalletService     = "wallet"
	WalletTypeService = "wallet_type"
)

// Message field logging constants
const (
	// --- env / startup ---
	LogEnvVarMissing = "env_var_missing"

	// --- infrastructure setup ---
	LogDBSetupSuccess       = "db_setup_success"
	LogRabbitmqSetupSuccess = "rabbitmq_setup_success"
	LogRabbitmqInitFailed   = "rabbitmq_init_failed"

	// --- outbox publisher ---
	LogOutboxPublisherStarted       = "outbox_publisher_started"
	LogOutboxPublishPendingFailed   = "outbox_publish_pending_failed"
	LogOutboxMessagePublishFailed   = "outbox_message_publish_failed"
	LogOutboxMessageMaxRetries      = "outbox_message_max_retries_exceeded"
	LogOutboxIncrementRetriesFailed = "outbox_increment_retries_failed"
	LogOutboxMarkPublishedFailed    = "outbox_mark_published_failed"
	LogOutboxMessagePublished       = "outbox_message_published"
	LogOutboxCleanupFailed          = "outbox_cleanup_failed"

	// --- grpc client ---
	LogGRPCClientSetupSuccess   = "grpc_client_setup_success"
	LogGRPCClientSetupFailed    = "grpc_client_setup_failed"
	LogGRPCClientShutdownFailed = "grpc_client_shutdown_failed"

	// --- grpc server ---
	LogGRPCServerStarted     = "grpc_server_started"
	LogGRPCServerSetupFailed = "grpc_server_setup_failed"
	LogGRPCServerServeFailed = "grpc_server_serve_failed"

	// --- http server ---
	LogHTTPServerStarted        = "http_server_started"
	LogHTTPServerStartFailed    = "http_server_start_failed"
	LogHTTPServerShutdownFailed = "http_server_shutdown_failed"

	// --- shutdown ---
	LogShutdownSignalReceived      = "shutdown_signal_received"
	LogShutdownCompleted           = "shutdown_completed"
	LogShutdownCompletedWithErrors = "shutdown_completed_with_errors"
	LogRabbitmqCloseFailed         = "rabbitmq_close_failed"
	LogDBCloseFailed               = "db_close_failed"

	// --- wallet (http handler) ---
	LogGetAllWalletsFailed               = "get_all_wallets_failed"
	LogGetWalletByIDFailed               = "get_wallet_by_id_failed"
	LogGetWalletsByUserIDFailed          = "get_wallets_by_user_id_failed"
	LogGetWalletsByUserIDGroupTypeFailed = "get_wallets_by_user_id_group_by_type_failed"
	LogCreateWalletBadRequest            = "create_wallet_bad_request"
	LogCreateWalletFailed                = "create_wallet_failed"
	LogCreateWalletGRPCFailedRollback    = "create_wallet_grpc_failed_will_rollback"
	LogWalletCreated                     = "wallet_created"
	LogUpdateWalletBadRequest            = "update_wallet_bad_request"
	LogUpdateWalletFailed                = "update_wallet_failed"
	LogDeleteWalletFailed                = "delete_wallet_failed"

	// --- wallet (grpc server) ---
	LogGetAllWalletsStreamFailed  = "get_all_wallets_stream_send_failed"
	LogGetUserWalletsFailed       = "get_user_wallets_failed"
	LogGetUserWalletsSuccess      = "get_user_wallets_success"
	LogGetUserWalletsStreamFailed = "get_user_wallets_stream_send_failed"
	LogUpdateWalletNotFound       = "update_wallet_not_found"
	LogUpdateWalletInvalidTypeID  = "update_wallet_invalid_wallet_type_id"
	LogWalletUpdated              = "wallet_updated"
	LogWalletDeleted              = "wallet_deleted"
	LogGetAllWalletTypesSuccess   = "get_all_wallet_types_success"
	LogGetWalletSummaryFailed     = "get_wallet_summary_failed"
	LogGetWalletSummarySuccess    = "get_wallet_summary_success"

	// --- wallet type (http handler) ---
	LogGetAllWalletTypesFailed    = "get_all_wallet_types_failed"
	LogGetWalletTypeByIDFailed    = "get_wallet_type_by_id_failed"
	LogCreateWalletTypeBadRequest = "create_wallet_type_bad_request"
	LogCreateWalletTypeFailed     = "create_wallet_type_failed"
	LogWalletTypeCreated          = "wallet_type_created"
	LogUpdateWalletTypeBadRequest = "update_wallet_type_bad_request"
	LogUpdateWalletTypeFailed     = "update_wallet_type_failed"
	LogDeleteWalletTypeFailed     = "delete_wallet_type_failed"

	// --- admin consumer ---
	AdminConsumerService      = "admin_consumer"
	LogAdminConsumerStarted   = "admin_consumer_started"
	LogAdminConsumerStopped   = "admin_consumer_stopped"
	LogAdminConsumerFailed    = "admin_consumer_failed"
	LogAdminEventHandleFailed = "admin_event_handle_failed"
	LogAdminEventUnknown      = "admin_event_unknown_action"
	LogAdminWalletTypeCreated = "admin_wallet_type_created"
	LogAdminWalletTypeUpdated = "admin_wallet_type_updated"
	LogAdminWalletTypeDeleted = "admin_wallet_type_deleted"

	// --- gRPC server (admin master data) ---
	LogListWalletTypesSuccess     = "list_wallet_types_success"
	LogListWalletTypesFailed      = "list_wallet_types_failed"
	LogGetWalletTypeDetailSuccess = "get_wallet_type_detail_success"
	LogGetWalletTypeDetailFailed  = "get_wallet_type_detail_failed"
)
