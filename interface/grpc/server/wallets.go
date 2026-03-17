package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"refina-wallet/config/log"
	"refina-wallet/internal/service"
	"refina-wallet/internal/types/dto"
	"refina-wallet/internal/utils/data"

	wpb "github.com/MuhammadMiftaa/Refina-Protobuf/wallet"
)

type walletServer struct {
	wpb.UnimplementedWalletServiceServer
	walletService      service.WalletsService
	walletTypesService service.WalletTypesService
}

// ── Helper: convert model wallet to proto Wallet ──

func walletToProto(w dto.WalletsResponse) *wpb.Wallet {
	return &wpb.Wallet{
		Id:             w.ID,
		UserId:         w.UserID,
		Name:           w.Name,
		Number:         w.Number,
		Balance:        w.Balance,
		WalletTypeId:   w.WalletTypeID,
		WalletType:     w.WalletType,
		WalletTypeName: w.WalletTypeName,
	}
}

func walletToProtoFull(w dto.WalletsResponse, txnCount int32, createdAt, updatedAt string) *wpb.Wallet {
	return &wpb.Wallet{
		Id:               w.ID,
		UserId:           w.UserID,
		Name:             w.Name,
		Number:           w.Number,
		Balance:          w.Balance,
		WalletTypeId:     w.WalletTypeID,
		WalletType:       w.WalletType,
		WalletTypeName:   w.WalletTypeName,
		TransactionCount: txnCount,
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
	}
}

// ── GetWallets (stream) — admin: all wallets ──

func (s *walletServer) GetWallets(req *wpb.GetWalletOptions, stream wpb.WalletService_GetWalletsServer) error {
	timeout, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	wallets, err := s.walletService.GetAllWallets(timeout)
	if err != nil {
		log.Error(data.LogGetAllWalletsFailed, map[string]any{
			"service": data.GRPCServerService,
			"error":   err.Error(),
		})
		return fmt.Errorf("get all wallets: %w", err)
	}

	for _, wallet := range wallets {
		if err := stream.Send(walletToProto(wallet)); err != nil {
			log.Error(data.LogGetAllWalletsStreamFailed, map[string]any{
				"service":   data.GRPCServerService,
				"wallet_id": wallet.ID,
				"error":     err.Error(),
			})
			return fmt.Errorf("stream send wallet [id=%s]: %w", wallet.ID, err)
		}
	}

	return nil
}

// ── GetUserWallets (unary) — user's wallets with full detail ──

func (s *walletServer) GetUserWallets(ctx context.Context, req *wpb.UserID) (*wpb.GetUserWalletsResponse, error) {
	userID := req.GetId()

	wallets, err := s.walletService.GetWalletsByUserID(ctx, userID)
	if err != nil {
		log.Error(data.LogGetUserWalletsFailed, map[string]any{
			"service": data.GRPCServerService,
			"user_id": userID,
			"error":   err.Error(),
		})
		return nil, fmt.Errorf("get wallets by user [id=%s]: %w", userID, err)
	}

	var protoWallets []*wpb.Wallet
	for _, wallet := range wallets {
		pw := &wpb.Wallet{
			Id:             wallet.ID,
			UserId:         wallet.UserID,
			Name:           wallet.Name,
			Number:         wallet.Number,
			Balance:        wallet.Balance,
			WalletTypeId:   wallet.WalletTypeID,
			WalletType:     wallet.WalletType,
			WalletTypeName: wallet.WalletTypeName,
			CreatedAt:      wallet.CreatedAt,
			UpdatedAt:      wallet.UpdatedAt,
			WalletTypeDetail: &wpb.WalletTypeDetail{
				Id:          wallet.WalletTypeID,
				Name:        wallet.WalletTypeName,
				Type:        wallet.WalletType,
				Description: wallet.WalletTypeDescription,
			},
		}
		protoWallets = append(protoWallets, pw)
	}

	log.Info(data.LogGetUserWalletsSuccess, map[string]any{
		"service": data.GRPCServerService,
		"user_id": userID,
		"count":   len(protoWallets),
	})

	return &wpb.GetUserWalletsResponse{Wallets: protoWallets}, nil
}

// ── GetWalletByID ──

func (s *walletServer) GetWalletByID(ctx context.Context, req *wpb.WalletID) (*wpb.Wallet, error) {
	walletID := req.GetId()

	wallet, err := s.walletService.GetWalletByID(ctx, walletID)
	if err != nil {
		log.Error(data.LogGetWalletByIDFailed, map[string]any{
			"service":   data.GRPCServerService,
			"wallet_id": walletID,
			"error":     err.Error(),
		})
		return nil, fmt.Errorf("get wallet [id=%s]: %w", walletID, err)
	}

	return &wpb.Wallet{
		Id:             wallet.ID,
		UserId:         wallet.UserID,
		Name:           wallet.Name,
		Number:         wallet.Number,
		Balance:        wallet.Balance,
		WalletTypeId:   wallet.WalletTypeID,
		WalletType:     wallet.WalletType,
		WalletTypeName: wallet.WalletTypeName,
		CreatedAt:      wallet.CreatedAt,
		UpdatedAt:      wallet.UpdatedAt,
		WalletTypeDetail: &wpb.WalletTypeDetail{
			Id:          wallet.WalletTypeID,
			Name:        wallet.WalletTypeName,
			Type:        wallet.WalletType,
			Description: wallet.WalletTypeDescription,
		},
	}, nil
}

// ── CreateWallet ──

func (s *walletServer) CreateWallet(ctx context.Context, req *wpb.CreateWalletRequest) (*wpb.Wallet, error) {
	userID := req.GetUserId()

	walletReq := dto.WalletsRequest{
		UserID:       userID,
		WalletTypeID: req.GetWalletTypeId(),
		Name:         req.GetName(),
		Number:       req.GetNumber(),
		Balance:      req.GetBalance(),
	}

	// The service handles tx management, outbox, and initial deposit via gRPC
	result, err := s.walletService.CreateWalletGRPC(ctx, walletReq)
	if err != nil {
		log.Error(data.LogCreateWalletFailed, map[string]any{
			"service": data.GRPCServerService,
			"user_id": userID,
			"error":   err.Error(),
		})
		return nil, fmt.Errorf("create wallet for user [id=%s]: %w", userID, err)
	}

	log.Info(data.LogWalletCreated, map[string]any{
		"service":   data.GRPCServerService,
		"wallet_id": result.ID,
		"user_id":   userID,
	})

	return walletToProto(result), nil
}

// ── UpdateWallet ──

func (s *walletServer) UpdateWallet(ctx context.Context, req *wpb.UpdateWalletRequest) (*wpb.Wallet, error) {
	walletID := req.GetId()

	walletReq := dto.WalletsRequest{
		Name:         req.GetName(),
		Number:       req.GetNumber(),
		WalletTypeID: req.GetWalletTypeId(),
		Balance:      req.GetBalance(),
	}

	result, err := s.walletService.UpdateWallet(ctx, walletID, walletReq)
	if err != nil {
		log.Error(data.LogUpdateWalletFailed, map[string]any{
			"service":   data.GRPCServerService,
			"wallet_id": walletID,
			"error":     err.Error(),
		})
		return nil, fmt.Errorf("update wallet [id=%s]: %w", walletID, err)
	}

	log.Info(data.LogWalletUpdated, map[string]any{
		"service":   data.GRPCServerService,
		"wallet_id": result.ID,
	})

	return walletToProto(result), nil
}

// ── DeleteWallet ──

func (s *walletServer) DeleteWallet(ctx context.Context, req *wpb.WalletID) (*wpb.Wallet, error) {
	walletID := req.GetId()

	result, err := s.walletService.DeleteWallet(ctx, walletID)
	if err != nil {
		log.Error(data.LogDeleteWalletFailed, map[string]any{
			"service":   data.GRPCServerService,
			"wallet_id": walletID,
			"error":     err.Error(),
		})
		return nil, fmt.Errorf("delete wallet [id=%s]: %w", walletID, err)
	}

	log.Info(data.LogWalletDeleted, map[string]any{
		"service":   data.GRPCServerService,
		"wallet_id": result.ID,
	})

	return walletToProto(result), nil
}

// ── GetWalletTypes ──

func (s *walletServer) GetWalletTypes(ctx context.Context, req *wpb.Empty) (*wpb.GetWalletTypesResponse, error) {
	walletTypes, err := s.walletTypesService.GetAllWalletTypes(ctx)
	if err != nil {
		log.Error(data.LogGetAllWalletTypesFailed, map[string]any{
			"service": data.GRPCServerService,
			"error":   err.Error(),
		})
		return nil, fmt.Errorf("get wallet types: %w", err)
	}

	var protoTypes []*wpb.WalletTypeDetail
	for _, wt := range walletTypes {
		protoTypes = append(protoTypes, &wpb.WalletTypeDetail{
			Id:          wt.ID,
			Name:        wt.Name,
			Type:        string(wt.Type),
			Description: wt.Description,
		})
	}

	log.Info(data.LogGetAllWalletTypesSuccess, map[string]any{
		"service": data.GRPCServerService,
		"count":   len(protoTypes),
	})

	return &wpb.GetWalletTypesResponse{WalletTypes: protoTypes}, nil
}

// ── GetWalletSummary ──

func (s *walletServer) GetWalletSummary(ctx context.Context, req *wpb.UserID) (*wpb.WalletSummary, error) {
	userID := req.GetId()

	wallets, err := s.walletService.GetWalletsByUserID(ctx, userID)
	if err != nil {
		log.Error(data.LogGetWalletSummaryFailed, map[string]any{
			"service": data.GRPCServerService,
			"user_id": userID,
			"error":   err.Error(),
		})
		return nil, fmt.Errorf("get wallet summary for user [id=%s]: %w", userID, err)
	}

	var totalBalance float64
	var totalTransactions int32
	for _, w := range wallets {
		totalBalance += w.Balance
	}

	log.Info(data.LogGetWalletSummarySuccess, map[string]any{
		"service":      data.GRPCServerService,
		"user_id":      userID,
		"wallet_count": len(wallets),
	})

	return &wpb.WalletSummary{
		TotalWallets:      int32(len(wallets)),
		TotalBalance:      totalBalance,
		TotalTransactions: totalTransactions,
	}, nil
}

// ── ListWalletTypes (admin paginated) ──

func (s *walletServer) ListWalletTypes(ctx context.Context, req *wpb.ListWalletTypesRequest) (*wpb.ListWalletTypesResponse, error) {
	page := int(req.GetPage())
	if page < 1 {
		page = 1
	}
	pageSize := int(req.GetPageSize())
	if pageSize < 1 {
		pageSize = 10
	}
	sortBy := req.GetSortBy()
	if sortBy == "" {
		sortBy = "created_at"
	}
	sortOrder := req.GetSortOrder()
	if sortOrder == "" {
		sortOrder = "desc"
	}
	search := req.GetSearch()

	allTypes, err := s.walletTypesService.GetAllWalletTypes(ctx)
	if err != nil {
		log.Error(data.LogListWalletTypesFailed, map[string]any{
			"service": data.GRPCServerService,
			"error":   err.Error(),
		})
		return nil, fmt.Errorf("list wallet types: %w", err)
	}

	// Filter by search
	var filtered []dto.WalletTypesResponse
	if search != "" {
		for _, wt := range allTypes {
			if containsIgnoreCase(wt.Name, search) || containsIgnoreCase(string(wt.Type), search) || containsIgnoreCase(wt.Description, search) {
				filtered = append(filtered, wt)
			}
		}
	} else {
		filtered = allTypes
	}

	total := int32(len(filtered))
	totalPages := total / int32(pageSize)
	if total%int32(pageSize) != 0 {
		totalPages++
	}

	// Sort
	sortWalletTypes(filtered, sortBy, sortOrder)

	// Paginate
	start := (page - 1) * pageSize
	end := start + pageSize
	if start > len(filtered) {
		start = len(filtered)
	}
	if end > len(filtered) {
		end = len(filtered)
	}
	paged := filtered[start:end]

	var protoTypes []*wpb.WalletTypeDetail
	for _, wt := range paged {
		protoTypes = append(protoTypes, &wpb.WalletTypeDetail{
			Id:          wt.ID,
			Name:        wt.Name,
			Type:        string(wt.Type),
			Description: wt.Description,
		})
	}

	log.Info(data.LogListWalletTypesSuccess, map[string]any{
		"service": data.GRPCServerService,
		"total":   total,
		"page":    page,
	})

	return &wpb.ListWalletTypesResponse{
		WalletTypes: protoTypes,
		Total:       total,
		Page:        int32(page),
		PageSize:    int32(pageSize),
		TotalPages:  totalPages,
	}, nil
}

// ── GetWalletTypeDetail ──

func (s *walletServer) GetWalletTypeDetail(ctx context.Context, req *wpb.WalletTypeID) (*wpb.WalletTypeDetail, error) {
	id := req.GetId()

	wt, err := s.walletTypesService.GetWalletTypeByID(ctx, id)
	if err != nil {
		log.Error(data.LogGetWalletTypeDetailFailed, map[string]any{
			"service":        data.GRPCServerService,
			"wallet_type_id": id,
			"error":          err.Error(),
		})
		return nil, fmt.Errorf("get wallet type detail [id=%s]: %w", id, err)
	}

	log.Info(data.LogGetWalletTypeDetailSuccess, map[string]any{
		"service":        data.GRPCServerService,
		"wallet_type_id": id,
	})

	return &wpb.WalletTypeDetail{
		Id:          wt.ID,
		Name:        wt.Name,
		Type:        string(wt.Type),
		Description: wt.Description,
	}, nil
}

// ── Helpers ──

func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(func() bool {
			sl := strings.ToLower(s)
			sub := strings.ToLower(substr)
			return strings.Contains(sl, sub)
		})())
}

func sortWalletTypes(types []dto.WalletTypesResponse, sortBy, sortOrder string) {
	sort.Slice(types, func(i, j int) bool {
		var cmp bool
		switch sortBy {
		case "name":
			cmp = strings.ToLower(types[i].Name) < strings.ToLower(types[j].Name)
		case "type":
			cmp = string(types[i].Type) < string(types[j].Type)
		default: // created_at, id
			cmp = types[i].ID < types[j].ID
		}
		if sortOrder == "desc" {
			return !cmp
		}
		return cmp
	})
}
