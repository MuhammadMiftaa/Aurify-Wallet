package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"refina-wallet/config/log"
	"refina-wallet/interface/queue"
	"refina-wallet/internal/service"
	"refina-wallet/internal/types/dto"
	"refina-wallet/internal/utils/data"

	"github.com/rabbitmq/amqp091-go"
)

type AdminEventConsumer struct {
	rabbitMQ          queue.RabbitMQClient
	walletTypeService service.WalletTypesService
}

func NewAdminEventConsumer(rmq queue.RabbitMQClient, wts service.WalletTypesService) *AdminEventConsumer {
	return &AdminEventConsumer{
		rabbitMQ:          rmq,
		walletTypeService: wts,
	}
}

func (c *AdminEventConsumer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info(data.LogAdminConsumerStopped, map[string]any{"service": data.AdminConsumerService})
			return
		default:
			if err := c.consume(ctx); err != nil {
				log.Error(data.LogAdminConsumerFailed, map[string]any{
					"service": data.AdminConsumerService,
					"error":   err.Error(),
				})
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
			}
		}
	}
}

func (c *AdminEventConsumer) consume(ctx context.Context) error {
	channel, err := c.rabbitMQ.GetChannel()
	if err != nil {
		return fmt.Errorf("get channel: %w", err)
	}
	defer channel.Close()

	// Declare admin exchange
	if err := channel.ExchangeDeclare(data.ADMIN_EXCHANGE, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	// Declare queue
	q, err := channel.QueueDeclare(data.ADMIN_QUEUE, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	// Bind queue to routing key
	if err := channel.QueueBind(q.Name, data.ADMIN_ROUTING_KEY, data.ADMIN_EXCHANGE, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	msgs, err := channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	log.Info(data.LogAdminConsumerStarted, map[string]any{"service": data.AdminConsumerService})

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("channel closed")
			}

			if err := c.handleMessage(ctx, msg); err != nil {
				log.Error(data.LogAdminEventHandleFailed, map[string]any{
					"service": data.AdminConsumerService,
					"error":   err.Error(),
				})
				_ = msg.Nack(false, true)
			} else {
				_ = msg.Ack(false)
			}
		}
	}
}

type adminEvent struct {
	Action      string                 `json:"action"`
	AggregateID string                 `json:"aggregate_id"`
	Data        map[string]interface{} `json:"data"`
	Timestamp   string                 `json:"timestamp"`
}

func (c *AdminEventConsumer) handleMessage(ctx context.Context, msg amqp091.Delivery) error {
	var event adminEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("unmarshal admin event: %w", err)
	}

	switch event.Action {
	case "create":
		return c.handleCreate(ctx, event)
	case "update":
		return c.handleUpdate(ctx, event)
	case "delete":
		return c.handleDelete(ctx, event)
	default:
		log.Warn(data.LogAdminEventUnknown, map[string]any{
			"service": data.AdminConsumerService,
			"action":  event.Action,
		})
		return nil
	}
}

func (c *AdminEventConsumer) handleCreate(ctx context.Context, event adminEvent) error {
	req := dto.WalletTypesRequest{
		Name:        getString(event.Data, "name"),
		Type:        dto.WalletType(getString(event.Data, "type")),
		Description: getString(event.Data, "description"),
	}

	_, err := c.walletTypeService.CreateWalletType(ctx, req)
	if err != nil {
		return fmt.Errorf("create wallet type: %w", err)
	}

	log.Info(data.LogAdminWalletTypeCreated, map[string]any{
		"service": data.AdminConsumerService,
		"name":    req.Name,
	})

	return nil
}

func (c *AdminEventConsumer) handleUpdate(ctx context.Context, event adminEvent) error {
	id := event.AggregateID
	req := dto.WalletTypesRequest{
		Name:        getString(event.Data, "name"),
		Type:        dto.WalletType(getString(event.Data, "type")),
		Description: getString(event.Data, "description"),
	}

	_, err := c.walletTypeService.UpdateWalletType(ctx, id, req)
	if err != nil {
		return fmt.Errorf("update wallet type [id=%s]: %w", id, err)
	}

	log.Info(data.LogAdminWalletTypeUpdated, map[string]any{
		"service":        data.AdminConsumerService,
		"wallet_type_id": id,
	})

	return nil
}

func (c *AdminEventConsumer) handleDelete(ctx context.Context, event adminEvent) error {
	id := event.AggregateID

	_, err := c.walletTypeService.DeleteWalletType(ctx, id)
	if err != nil {
		return fmt.Errorf("delete wallet type [id=%s]: %w", id, err)
	}

	log.Info(data.LogAdminWalletTypeDeleted, map[string]any{
		"service":        data.AdminConsumerService,
		"wallet_type_id": id,
	})

	return nil
}

func getString(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}
