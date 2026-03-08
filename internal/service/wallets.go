package service

import (
	"context"
	"encoding/json"
	"fmt"

	"refina-wallet/config/log"
	"refina-wallet/interface/grpc/client"
	"refina-wallet/interface/queue"
	"refina-wallet/internal/repository"
	"refina-wallet/internal/types/dto"
	"refina-wallet/internal/types/model"
	"refina-wallet/internal/types/view"
	"refina-wallet/internal/utils"
	"refina-wallet/internal/utils/data"

	tpb "github.com/MuhammadMiftaa/Refina-Protobuf/transaction"

	"github.com/google/uuid"
)

type WalletsService interface {
	GetAllWallets(ctx context.Context) ([]dto.WalletsResponse, error)
	GetWalletByID(ctx context.Context, id string) (dto.WalletsResponse, error)
	GetWalletsByUserID(ctx context.Context, userID string) ([]dto.WalletsResponse, error)
	GetWalletsByUserIDGroupByType(ctx context.Context, userID string) ([]view.ViewUserWalletsGroupByType, error)
	CreateWallet(ctx context.Context, userID string, wallet dto.WalletsRequest) (dto.WalletsResponse, error)
	CreateWalletGRPC(ctx context.Context, wallet dto.WalletsRequest) (dto.WalletsResponse, error)
	UpdateWallet(ctx context.Context, id string, wallet dto.WalletsRequest) (dto.WalletsResponse, error)
	DeleteWallet(ctx context.Context, id string) (dto.WalletsResponse, error)
}

type walletsService struct {
	txManager             repository.TxManager
	walletsRepository     repository.WalletsRepository
	walletTypesRepository repository.WalletTypesRepository
	outboxRepository      repository.OutboxRepository
	transactionClient     client.TransactionClient
	queue                 queue.RabbitMQClient
}

func NewWalletService(
	txManager repository.TxManager,
	walletsRepository repository.WalletsRepository,
	walletTypesRepository repository.WalletTypesRepository,
	outboxRepository repository.OutboxRepository,
	transactionRepository client.TransactionClient,
	queue queue.RabbitMQClient,
) WalletsService {
	return &walletsService{
		txManager:             txManager,
		walletsRepository:     walletsRepository,
		walletTypesRepository: walletTypesRepository,
		outboxRepository:      outboxRepository,
		transactionClient:     transactionRepository,
		queue:                 queue,
	}
}

func (wallet_serv *walletsService) GetAllWallets(ctx context.Context) ([]dto.WalletsResponse, error) {
	wallets, err := wallet_serv.walletsRepository.GetAllWallets(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("get all wallets: %w", err)
	}

	var walletsResponse []dto.WalletsResponse
	for _, wallet := range wallets {
		walletResponse := utils.ConvertToResponseType(wallet).(dto.WalletsResponse)
		walletsResponse = append(walletsResponse, walletResponse)
	}

	return walletsResponse, nil
}

func (wallet_serv *walletsService) GetWalletByID(ctx context.Context, id string) (dto.WalletsResponse, error) {
	wallet, err := wallet_serv.walletsRepository.GetWalletByID(ctx, nil, id)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("get wallet [id=%s]: %w", id, err)
	}

	walletResponse := utils.ConvertToResponseType(wallet).(dto.WalletsResponse)

	return walletResponse, nil
}

func (wallet_serv *walletsService) GetWalletsByUserID(ctx context.Context, userID string) ([]dto.WalletsResponse, error) {
	wallets, err := wallet_serv.walletsRepository.GetWalletsByUserID(ctx, nil, userID)
	if err != nil {
		return nil, fmt.Errorf("get wallets by user [id=%s]: %w", userID, err)
	}

	var walletsResponse []dto.WalletsResponse
	for _, wallet := range wallets {
		walletResponse := utils.ConvertToResponseType(wallet).(dto.WalletsResponse)
		walletsResponse = append(walletsResponse, walletResponse)
	}

	return walletsResponse, nil
}

func (wallet_serv *walletsService) GetWalletsByUserIDGroupByType(ctx context.Context, userID string) ([]view.ViewUserWalletsGroupByType, error) {
	wallets, err := wallet_serv.walletsRepository.GetWalletsByUserIDGroupByType(ctx, nil, userID)
	if err != nil {
		return nil, err
	}

	return wallets, err
}

func (wallet_serv *walletsService) CreateWallet(ctx context.Context, userID string, wallet dto.WalletsRequest) (dto.WalletsResponse, error) {
	UserID, err := utils.ParseUUID(userID)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("invalid user id: %w", err)
	}

	WalletTypeID, err := utils.ParseUUID(wallet.WalletTypeID)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("invalid wallet type id: %w", err)
	}

	walletType, err := wallet_serv.walletTypesRepository.GetWalletTypeByID(ctx, nil, wallet.WalletTypeID)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("wallet type not found [id=%s]: %w", wallet.WalletTypeID, err)
	}

	tx, err := wallet_serv.txManager.Begin(ctx)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: begin transaction: %w", err)
	}

	initialDeposit := new(tpb.TransactionDetail)

	defer func() {
		tx.Rollback()
		if err != nil && initialDeposit != nil && initialDeposit.GetId() != "" {
			// GRPC call
			wallet_serv.transactionClient.CancelInitialDeposit(ctx, initialDeposit.GetId())
		}
	}()

	// Create wallet
	walletID := uuid.New()
	newWallet, err := wallet_serv.walletsRepository.CreateWallet(ctx, tx, model.Wallets{
		Base: model.Base{
			ID: walletID,
		},
		UserID:       UserID,
		WalletTypeID: WalletTypeID,
		Name:         wallet.Name,
		Number:       wallet.Number,
		Balance:      wallet.Balance,
		WalletType:   walletType,
	})
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: insert to db: %w", err)
	}

	// GRPC call
	initialDeposit, err = wallet_serv.transactionClient.InitialDeposit(ctx, walletID.String(), wallet.Balance)
	if err != nil {
		log.Warn(data.LogCreateWalletGRPCFailedRollback, map[string]any{
			"service":   data.WalletService,
			"wallet_id": walletID.String(),
			"amount":    wallet.Balance,
			"error":     err.Error(),
		})
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: initial deposit via grpc: %w", err)
	}

	walletResponse := utils.ConvertToResponseType(newWallet).(dto.WalletsResponse)

	payload, err := json.Marshal(walletResponse)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: marshal wallet response: %w", err)
	}

	outboxMsg := &model.OutboxMessage{
		AggregateID: walletResponse.ID,
		EventType:   data.OUTBOX_EVENT_WALLET_CREATED,
		Payload:     payload,
		Published:   false,
		MaxRetries:  data.OUTBOX_PUBLISH_MAX_RETRIES,
	}

	if err := wallet_serv.outboxRepository.Create(ctx, tx, outboxMsg); err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: save outbox message: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: commit transaction: %w", err)
	}

	return walletResponse, nil
}

// CreateWalletGRPC is used by the gRPC server — user_id is already validated by the BFF
func (wallet_serv *walletsService) CreateWalletGRPC(ctx context.Context, wallet dto.WalletsRequest) (dto.WalletsResponse, error) {
	UserID, err := utils.ParseUUID(wallet.UserID)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("invalid user id: %w", err)
	}

	WalletTypeID, err := utils.ParseUUID(wallet.WalletTypeID)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("invalid wallet type id: %w", err)
	}

	walletType, err := wallet_serv.walletTypesRepository.GetWalletTypeByID(ctx, nil, wallet.WalletTypeID)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("wallet type not found [id=%s]: %w", wallet.WalletTypeID, err)
	}

	tx, err := wallet_serv.txManager.Begin(ctx)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: begin transaction: %w", err)
	}

	initialDeposit := new(tpb.TransactionDetail)

	defer func() {
		tx.Rollback()
		if err != nil && initialDeposit != nil && initialDeposit.GetId() != "" {
			wallet_serv.transactionClient.CancelInitialDeposit(ctx, initialDeposit.GetId())
		}
	}()

	walletID := uuid.New()
	newWallet, err := wallet_serv.walletsRepository.CreateWallet(ctx, tx, model.Wallets{
		Base: model.Base{
			ID: walletID,
		},
		UserID:       UserID,
		WalletTypeID: WalletTypeID,
		Name:         wallet.Name,
		Number:       wallet.Number,
		Balance:      wallet.Balance,
		WalletType:   walletType,
	})
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: insert to db: %w", err)
	}

	if wallet.Balance > 0 {
		initialDeposit, err = wallet_serv.transactionClient.InitialDeposit(ctx, walletID.String(), wallet.Balance)
		if err != nil {
			log.Warn(data.LogCreateWalletGRPCFailedRollback, map[string]any{
				"service":   data.WalletService,
				"wallet_id": walletID.String(),
				"amount":    wallet.Balance,
				"error":     err.Error(),
			})
			return dto.WalletsResponse{}, fmt.Errorf("create wallet: initial deposit via grpc: %w", err)
		}
	}

	walletResponse := utils.ConvertToResponseType(newWallet).(dto.WalletsResponse)

	payload, err := json.Marshal(walletResponse)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: marshal wallet response: %w", err)
	}

	outboxMsg := &model.OutboxMessage{
		AggregateID: walletResponse.ID,
		EventType:   data.OUTBOX_EVENT_WALLET_CREATED,
		Payload:     payload,
		Published:   false,
		MaxRetries:  data.OUTBOX_PUBLISH_MAX_RETRIES,
	}

	if err := wallet_serv.outboxRepository.Create(ctx, tx, outboxMsg); err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: save outbox message: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("create wallet: commit transaction: %w", err)
	}

	return walletResponse, nil
}

func (wallet_serv *walletsService) UpdateWallet(ctx context.Context, id string, wallet dto.WalletsRequest) (dto.WalletsResponse, error) {
	existingWallet, err := wallet_serv.walletsRepository.GetWalletByID(ctx, nil, id)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("wallet not found [id=%s]: %w", id, err)
	}

	existingWallet.Name = wallet.Name
	existingWallet.Number = wallet.Number
	existingWallet.Balance = wallet.Balance

	tx, err := wallet_serv.txManager.Begin(ctx)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("update wallet: begin transaction: %w", err)
	}

	defer func() {
		tx.Rollback()
	}()

	walletUpdated, err := wallet_serv.walletsRepository.UpdateWallet(ctx, tx, existingWallet)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("update wallet: update in db: %w", err)
	}

	walletResponse := utils.ConvertToResponseType(walletUpdated).(dto.WalletsResponse)

	payload, err := json.Marshal(walletResponse)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("update wallet: marshal wallet response: %w", err)
	}

	outboxMsg := &model.OutboxMessage{
		AggregateID: walletResponse.ID,
		EventType:   data.OUTBOX_EVENT_WALLET_UPDATED,
		Payload:     payload,
		Published:   false,
		MaxRetries:  data.OUTBOX_PUBLISH_MAX_RETRIES,
	}

	if err := wallet_serv.outboxRepository.Create(ctx, tx, outboxMsg); err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("update wallet: save outbox message: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("update wallet: commit transaction: %w", err)
	}

	return walletResponse, nil
}

func (wallet_serv *walletsService) DeleteWallet(ctx context.Context, id string) (dto.WalletsResponse, error) {
	existingWallet, err := wallet_serv.walletsRepository.GetWalletByID(ctx, nil, id)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("wallet not found [id=%s]: %w", id, err)
	}

	if existingWallet.Balance != 0 {
		return dto.WalletsResponse{}, fmt.Errorf("wallet balance must be zero before deletion")
	}

	tx, err := wallet_serv.txManager.Begin(ctx)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("delete wallet: begin transaction: %w", err)
	}

	defer func() {
		tx.Rollback()
	}()

	deletedWallet, err := wallet_serv.walletsRepository.DeleteWallet(ctx, tx, existingWallet)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("delete wallet: delete from db: %w", err)
	}

	walletResponse := utils.ConvertToResponseType(deletedWallet).(dto.WalletsResponse)

	payload, err := json.Marshal(walletResponse)
	if err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("delete wallet: marshal wallet response: %w", err)
	}

	outboxMsg := &model.OutboxMessage{
		AggregateID: walletResponse.ID,
		EventType:   data.OUTBOX_EVENT_WALLET_DELETED,
		Payload:     payload,
		Published:   false,
		MaxRetries:  data.OUTBOX_PUBLISH_MAX_RETRIES,
	}

	if err := wallet_serv.outboxRepository.Create(ctx, tx, outboxMsg); err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("delete wallet: save outbox message: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return dto.WalletsResponse{}, fmt.Errorf("delete wallet: commit transaction: %w", err)
	}

	return walletResponse, nil
}
