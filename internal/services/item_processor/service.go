// The service is designed for sending items using the Fan-In mechanism.
// The Process method is thread-safe and can be used in multiple goroutines.
package itemprocessor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	batch_processor_client "onmi/internal/clients/item_processor"

	"golang.org/x/time/rate"
)

var ErrProcessTimeout = errors.New("process timeout")

const (
	bufferChSizeDefault     = 0
	processTimeoutDefault   = 10 * 360 * 24 * time.Hour
	flushEveryDefault       = 10 * time.Second
	lastFlushTimeoutDefault = 1 * time.Second
)

type Batch struct {
	Items []Item
}

type Item struct{}

//go:generate mockgen -source=$GOFILE -destination=./mocks/service_mocks.gen.go -package=itemprocessor_mocks
type itemProcessorClient interface {
	GetLimits() (n uint64, p time.Duration)
	Process(ctx context.Context, batch batch_processor_client.Batch) error
}

type Service struct {
	lg *slog.Logger

	itemProcessorClient itemProcessorClient

	ch     chan Item
	chSize int

	buffer     []Item
	bufferSize int

	processTimeout time.Duration
	flushEvery     time.Duration

	limiter *rate.Limiter
}

type optionFunc func(s *Service)

// WithChannelBufferSize sets underlying communication channel size.
// Default: 0.
func WithChannelBufferSize(channelBufferSize int) optionFunc {
	return func(s *Service) {
		s.chSize = channelBufferSize
	}
}

// WithFlushEvery sets buffer flush period.
// Default: 10s.
func WithFlushEvery(timeout time.Duration) optionFunc {
	return func(s *Service) {
		s.flushEvery = timeout
	}
}

// WithProcessTimeout sets process timeout.
// Default: Inf.
func WithProcessTimeout(timeout time.Duration) optionFunc {
	return func(s *Service) {
		s.processTimeout = timeout
	}
}

func New(itemProcessorClient itemProcessorClient, lg *slog.Logger, opts ...optionFunc) *Service {
	numItemsPerBatch, period := itemProcessorClient.GetLimits()

	s := &Service{
		lg:                  lg,
		itemProcessorClient: itemProcessorClient,
		chSize:              bufferChSizeDefault,
		bufferSize:          int(numItemsPerBatch),
		processTimeout:      processTimeoutDefault,
		flushEvery:          flushEveryDefault,
		limiter: rate.NewLimiter(rate.Limit(
			float64(numItemsPerBatch)/period.Seconds(),
		), int(numItemsPerBatch)),
	}

	for _, o := range opts {
		o(s)
	}

	s.buffer = make([]Item, 0, s.bufferSize)
	s.ch = make(chan Item, s.chSize)

	return s
}

// Run initiates the processing of items.
// Returns *processError, which can be unpacked through UnpackProcessError to
// obtain a []Item that failed to be processed using the *processError.Items() method.
// Returns nil if all items were successfully processed.
// Use WithFlushEvery to control flush period.
func (s *Service) Run(ctx context.Context) (err error) {
	defer func() {
		deferCtx, cancel := context.WithTimeout(context.Background(), lastFlushTimeoutDefault)
		defer cancel()

		if e := s.flush(deferCtx); e != nil {
			err = errors.Join(fmt.Errorf("flush on defer: %w", e))
			return
		}
		s.lg.Info("defer buffer flushed")
	}()

	ticker := time.NewTicker(s.flushEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case item := <-s.ch:
			s.buffer = append(s.buffer, item)

			if len(s.buffer) < s.bufferSize {
				continue
			}

			err := s.flush(ctx)
			if nil == err {
				continue
			}
			if errors.Is(err, context.Canceled) {
				return nil
			}
			s.lg.Error("flush on write", slog.Any("error", err))

		case <-ticker.C:
			err := s.flush(ctx)
			if nil == err {
				continue
			}
			if errors.Is(err, context.Canceled) {
				return nil
			}
			s.lg.Error("flush on tick", slog.Any("error", err))
		}
	}
}

// Process sends the item to the buffer for processing. The operation may block.
// Returns ErrProcessTimeout if timeout reached.
// Use WithProcessTimeout to control the timeout.
// Use WithChannelBufferSize to control underlying buffer for transmitting items.
func (s *Service) Process(ctx context.Context, item Item) error {
	ctx, cancel := context.WithTimeout(ctx, s.processTimeout)
	defer cancel()

	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ErrProcessTimeout
		}
		return nil
	case s.ch <- item:
	}
	return nil
}

func (s *Service) flush(ctx context.Context) error {
	if len(s.buffer) == 0 {
		return nil
	}

	if !s.limiter.AllowN(time.Now(), len(s.buffer)) {
		s.lg.Info("limit reached")
		err := s.limiter.WaitN(ctx, len(s.buffer))
		if err != nil {
			return fmt.Errorf("limiter wait: %w", newProcessError(err, s.buffer))
		}
	}

	// TODO: add a retry mechanism if client does not provide it,
	// preferably with a circuit breaker mechanism.
	err := s.itemProcessorClient.Process(ctx, batch_processor_client.Batch(
		adaptItemsToClientItems(s.buffer),
	))
	if err != nil {
		return fmt.Errorf("batch process: %w", newProcessError(err, s.buffer))
	}

	s.lg.Info("buffer flushed", slog.Int("count", len(s.buffer)))
	s.buffer = s.buffer[:0]

	return nil
}

// Close clean-ups resources.
func (s *Service) Close() error {
	close(s.ch)
	return nil
}

func UnpackProcessError(err error) *processError {
	pe := new(processError)
	if errors.As(err, &pe) {
		return pe
	}
	return nil
}

type processError struct {
	err   error
	items []Item
}

func newProcessError(err error, items []Item) *processError {
	return &processError{
		err:   err,
		items: items,
	}
}

func (pe *processError) Unwrap() error {
	return pe.err
}

func (pe *processError) Items() []Item {
	return pe.items
}

func (pe *processError) Error() string {
	return pe.err.Error()
}

func adaptItemsToClientItems(items []Item) []batch_processor_client.Item {
	res := make([]batch_processor_client.Item, 0, len(items))
	for i := 0; i < len(items); i++ {
		res = append(res, adaptItemToClientItem(items[i]))
	}
	return res
}

func adaptItemToClientItem(_ Item) batch_processor_client.Item {
	return batch_processor_client.Item{}
}
