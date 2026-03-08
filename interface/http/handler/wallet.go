package handler

import (
	"net/http"
	"strings"

	"refina-wallet/config/log"
	"refina-wallet/interface/grpc/interceptor"
	"refina-wallet/internal/service"
	"refina-wallet/internal/types/dto"
	"refina-wallet/internal/utils/data"

	"github.com/gin-gonic/gin"
)

type walletHandler struct {
	walletService service.WalletsService
}

func NewWalletHandler(walletService service.WalletsService) *walletHandler {
	return &walletHandler{walletService}
}

func (wallet_handler *walletHandler) GetAllWallets(c *gin.Context) {
	ctx := c.Request.Context()
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	wallets, err := wallet_handler.walletService.GetAllWallets(ctx)
	if err != nil {
		log.Error(data.LogGetAllWalletsFailed, map[string]any{
			"service":    data.WalletService,
			"request_id": requestID,
			"error":      err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Get all wallets",
		"data":       wallets,
	})
}

func (wallet_handler *walletHandler) GetWalletByID(c *gin.Context) {
	ctx := c.Request.Context()
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	id := c.Param("id")

	wallet, err := wallet_handler.walletService.GetWalletByID(ctx, id)
	if err != nil {
		log.Error(data.LogGetWalletByIDFailed, map[string]any{
			"service":    data.WalletService,
			"request_id": requestID,
			"wallet_id":  id,
			"error":      err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Get wallet by ID",
		"data":       wallet,
	})
}

func (wallet_handler *walletHandler) GetWalletsByUserID(c *gin.Context) {
	ctx := c.Request.Context()
	userID := interceptor.UserIDFromContext(ctx)
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	userWallets, err := wallet_handler.walletService.GetWalletsByUserID(ctx, userID)
	if err != nil {
		log.Error(data.LogGetWalletsByUserIDFailed, map[string]any{
			"service":    data.WalletService,
			"request_id": requestID,
			"error":      err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Get user wallets data",
		"data":       userWallets,
	})
}

func (wallet_handler *walletHandler) GetWalletsByUserIDGroupByType(c *gin.Context) {
	ctx := c.Request.Context()
	userID := interceptor.UserIDFromContext(ctx)
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	userWallets, err := wallet_handler.walletService.GetWalletsByUserIDGroupByType(ctx, userID)
	if err != nil {
		log.Error(data.LogGetWalletsByUserIDGroupTypeFailed, map[string]any{
			"service":    data.WalletService,
			"request_id": requestID,
			"error":      err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Get user wallets data",
		"data":       userWallets,
	})
}

func (wallet_handler *walletHandler) CreateWallet(c *gin.Context) {
	ctx := c.Request.Context()
	userID := interceptor.UserIDFromContext(ctx)
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	var walletRequest dto.WalletsRequest
	if err := c.ShouldBindJSON(&walletRequest); err != nil {
		log.Warn(data.LogCreateWalletBadRequest, map[string]any{
			"service":    data.WalletService,
			"request_id": requestID,
			"error":      err.Error(),
		})
		c.JSON(http.StatusBadRequest, gin.H{
			"statusCode": 400,
			"status":     false,
			"message":    "invalid request body",
		})
		return
	}

	wallet, err := wallet_handler.walletService.CreateWallet(ctx, userID, walletRequest)
	if err != nil {
		log.Error(data.LogCreateWalletFailed, map[string]any{
			"service":        data.WalletService,
			"request_id":     requestID,
			"wallet_type_id": walletRequest.WalletTypeID,
			"error":          err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	log.Info(data.LogWalletCreated, map[string]any{
		"service":    data.WalletService,
		"request_id": requestID,
		"wallet_id":  wallet.ID,
		"user_id":    wallet.UserID,
		"type":       wallet.WalletType,
	})

	c.JSON(http.StatusCreated, gin.H{
		"statusCode": 201,
		"status":     true,
		"message":    "Create wallet",
		"data":       wallet,
	})
}

func (wallet_handler *walletHandler) UpdateWallet(c *gin.Context) {
	ctx := c.Request.Context()
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	id := c.Param("id")

	var walletRequest dto.WalletsRequest
	if err := c.ShouldBindJSON(&walletRequest); err != nil {
		log.Warn(data.LogUpdateWalletBadRequest, map[string]any{
			"service":    data.WalletService,
			"request_id": requestID,
			"wallet_id":  id,
			"error":      err.Error(),
		})
		c.JSON(http.StatusBadRequest, gin.H{
			"statusCode": 400,
			"status":     false,
			"message":    "invalid request body",
		})
		return
	}

	wallet, err := wallet_handler.walletService.UpdateWallet(ctx, id, walletRequest)
	if err != nil {
		log.Error(data.LogUpdateWalletFailed, map[string]any{
			"service":    data.WalletService,
			"request_id": requestID,
			"wallet_id":  id,
			"error":      err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Update wallet",
		"data":       wallet,
	})
}

func (wallet_handler *walletHandler) DeleteWallet(c *gin.Context) {
	ctx := c.Request.Context()
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	id := c.Param("id")

	wallet, err := wallet_handler.walletService.DeleteWallet(ctx, id)
	if err != nil {
		log.Error(data.LogDeleteWalletFailed, map[string]any{
			"service":    data.WalletService,
			"request_id": requestID,
			"wallet_id":  id,
			"error":      err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Delete wallet",
		"data":       wallet,
	})
}

// mapServiceError menerjemahkan error dari service ke HTTP status + pesan aman untuk client
func mapServiceError(err error) (int, string) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return http.StatusNotFound, "resource not found"
	case strings.Contains(msg, "invalid user id"),
		strings.Contains(msg, "invalid wallet type id"):
		return http.StatusBadRequest, "invalid request"
	case strings.Contains(msg, "balance must be zero"):
		return http.StatusUnprocessableEntity, "wallet balance must be zero before deletion"
	default:
		return http.StatusInternalServerError, "internal server error"
	}
}
