package itemprocessor_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	batch_processor_client "onmi/internal/clients/item_processor"
	item_processor "onmi/internal/services/item_processor"
	item_processor_mocks "onmi/internal/services/item_processor/mocks"
)

type ItemProcessorSuite struct {
	suite.Suite

	Ctx       context.Context
	ctxCancel context.CancelFunc

	ctrl *gomock.Controller
	lg   *slog.Logger

	itemProcessorClientMock *item_processor_mocks.MockitemProcessorClient
	itemProcessorClientStub *itemProcessorClient // in-memory stub.

	itemProcessor *item_processor.Service
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestItemProcessorSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ItemProcessorSuite))
}

func (s *ItemProcessorSuite) SetupTest() {
	s.Ctx, s.ctxCancel = context.WithCancel(context.Background())
	s.ctrl = gomock.NewController(s.T())

	s.lg = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{}))

	s.itemProcessorClientStub = newItemProcessorClient(uint64(10), 100*time.Millisecond) // 100 rps
	s.itemProcessor = item_processor.New(
		s.itemProcessorClientStub,
		s.lg,
		item_processor.WithProcessTimeout(1*time.Second),
	)
}

func (s *ItemProcessorSuite) TearDownTest() {
	s.ctrl.Finish()
	s.Require().NoError(s.itemProcessor.Close())
	s.ctxCancel()
}

func (s *ItemProcessorSuite) TestProcess() {
	// Arrange.
	itemsToProceed := 100
	workersNum := 3
	totalItems := 0
	mx := new(sync.Mutex)

	cancel, errCh := s.runProcessor()
	defer cancel()

	workerFunc := func() error {
		items := s.generateItems(100)

		for _, item := range items {
			err := s.itemProcessor.Process(s.Ctx, item)
			if err != nil {
				return err
			}

			mx.Lock()
			totalItems++
			mx.Unlock()
		}
		return nil
	}

	// Action.
	eg, _ := errgroup.WithContext(s.Ctx)
	for i := 0; i < 3; i++ {
		eg.Go(workerFunc)
	}

	// Assert.
	s.Require().NoError(eg.Wait())

	cancel()

	s.Require().NoError(<-errCh)
	s.Require().EqualValues(itemsToProceed*workersNum, totalItems)
}

func (s *ItemProcessorSuite) TestProcessTimeout() {
	items := s.generateItems(1)
	err := s.itemProcessor.Process(s.Ctx, items[0])
	s.Require().ErrorIs(err, item_processor.ErrProcessTimeout)
}

func (s *ItemProcessorSuite) TestItemProcessorClientError() {
	// Arrange.
	s.itemProcessorClientMock = item_processor_mocks.NewMockitemProcessorClient(s.ctrl)
	s.itemProcessorClientMock.EXPECT().GetLimits().Return(uint64(10), 100*time.Millisecond) // 100 rps
	s.itemProcessor = item_processor.New(
		s.itemProcessorClientMock,
		s.lg,
		item_processor.WithProcessTimeout(1*time.Second),
	)

	items := s.generateItems(100)

	s.itemProcessorClientMock.EXPECT().Process(gomock.Any(), gomock.Any()).Times(9)
	s.itemProcessorClientMock.EXPECT().Process(gomock.Any(), gomock.Any()).Return(errors.New("unexpected"))

	cancel, errCh := s.runProcessor()
	defer cancel()

	// Action.
	for _, item := range items {
		err := s.itemProcessor.Process(s.Ctx, item)
		s.Require().NoError(err)
	}

	cancel()

	err := <-errCh
	s.Require().Error(err)

	processError := item_processor.UnpackProcessError(err)
	s.Require().NotNil(processError)
	s.Require().Len(processError.Items(), 10)
}

func (s *ItemProcessorSuite) runProcessor() (context.CancelFunc, <-chan error) {
	s.T().Helper()

	ctx, cancel := context.WithCancel(s.Ctx)

	errCh := make(chan error, 1)
	go func() {
		s.Require().NotPanics(func() {
			errCh <- s.itemProcessor.Run(ctx)
		})
	}()

	return cancel, errCh
}

func (s *ItemProcessorSuite) generateItems(n int) (res []item_processor.Item) {
	s.T().Helper()
	for i := 0; i < n; i++ {
		res = append(res, item_processor.Item{})
	}
	return res
}

type itemProcessorClient struct {
	n uint64
	p time.Duration

	limiter *rate.Limiter
}

func newItemProcessorClient(n uint64, p time.Duration) *itemProcessorClient {
	limiter := rate.NewLimiter(rate.Limit(float64(n)/p.Seconds()), 1)
	return &itemProcessorClient{
		n:       n,
		p:       p,
		limiter: limiter,
	}
}

func (ipc *itemProcessorClient) GetLimits() (n uint64, p time.Duration) {
	return ipc.n, ipc.p
}

func (ipc *itemProcessorClient) Process(_ context.Context, batch batch_processor_client.Batch) error {
	if !ipc.limiter.Allow() {
		panic("limit reached")
	}
	return nil
}
