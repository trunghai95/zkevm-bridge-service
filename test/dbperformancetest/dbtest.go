package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/0xPolygonHermez/zkevm-bridge-service/config"
	"github.com/0xPolygonHermez/zkevm-bridge-service/db"
	"github.com/0xPolygonHermez/zkevm-bridge-service/db/pgstorage"
	"github.com/0xPolygonHermez/zkevm-bridge-service/etherman"
	"github.com/0xPolygonHermez/zkevm-node/log"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v4"
	"math/big"
	"math/rand"
	"time"
)

const (
	configFilePath   = "./config.toml"
	transactionCount = 5000000
	claimCount       = 4000000
	blockCount       = 50000000
	userCount        = 5000
	logBatch         = 10000
	testIteration    = 1000
)

var (
	isPrepare = flag.Bool("prepare", false, "If prepare is true, run the prepare code to populate the data")
	isDryRun  = flag.Bool("dryrun", true, "")
	resetDB   = flag.Bool("reset", false, "If this is true, reset the DB to clean up the data before prepare data")
)

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	c, err := config.Load(configFilePath, "")
	if err != nil {
		panic(err)
	}
	log.Init(c.Log)

	err = db.RunMigrations(c.SyncDB)
	if err != nil {
		log.Errorf("Run migration error: %v", err)
		return
	}

	storageInterface, err := db.NewStorage(c.SyncDB)
	if err != nil {
		log.Errorf("New DB storage error: %v", err)
		return
	}

	ctx := context.Background()
	storage := storageInterface.(*pgstorage.PostgresStorage)
	dbTx, err := storage.Begin(ctx)
	// Won't commit test data
	defer func() {
		if *isDryRun {
			log.Info("Exiting, rolling back...")
			err = dbTx.Rollback(ctx)
		} else {
			log.Info("Committing...")
			err = dbTx.Commit(ctx)
		}
		if err != nil {
			log.Errorf("Commit/Rollback error: %v", err)
		}
	}()

	if err != nil {
		log.Errorf("begin transaction error: %v", err)
		return
	}

	if *isPrepare {
		if *resetDB {
			cfg := c.SyncDB
			pgCfg := pgstorage.Config{
				Name:     cfg.Name,
				User:     cfg.User,
				Password: cfg.Password,
				Host:     cfg.Host,
				Port:     cfg.Port,
			}
			err := pgstorage.InitOrReset(pgCfg)
			if err != nil {
				log.Errorf("InitOrReset DB err: %v", err)
				return
			}
		}

		err = prepareData(ctx, storage, dbTx)
		if err != nil {
			log.Errorf("prepareData error: %v", err)
			return
		}
	} else {
		runTest(ctx, storage, dbTx)
	}
}

func prepareData(ctx context.Context, storage *pgstorage.PostgresStorage, dbTx pgx.Tx) error {
	depositCnt := uint(0)
	blockNumber := uint64(0)
	// Add transactionCount deposits and claims to the DB
	for i := 1; i <= transactionCount; i++ {
		destAddr := getRandomAddress()
		if i%logBatch == 0 {
			log.Infof("Adding transaction #%d, destAddr %s", i, destAddr)
		}
		// Add deposit block
		blockNumber++
		block := &etherman.Block{
			BlockNumber: blockNumber,
			NetworkID:   0,
			BlockHash:   getHashFromInt(blockNumber),
			ReceivedAt:  time.Now(),
		}
		blockId, err := storage.AddBlock(ctx, block, dbTx)
		if err != nil {
			return err
		}

		// Add deposit
		depositCnt++
		deposit := &etherman.Deposit{
			OriginalNetwork:    0,
			DestinationNetwork: 1,
			DestinationAddress: destAddr,
			DepositCount:       depositCnt,
			BlockID:            blockId,
			BlockNumber:        blockNumber,
			NetworkID:          0,
			ReadyForClaim:      true,
			Metadata:           []byte(""),
			Time:               time.Now(),
		}
		_, err = storage.AddDeposit(ctx, deposit, dbTx)
		if err != nil {
			return err
		}

		if i > claimCount {
			continue
		}

		// Add claim block
		blockNumber++
		block = &etherman.Block{
			BlockNumber: blockNumber,
			NetworkID:   1,
			BlockHash:   getHashFromInt(blockNumber),
			ReceivedAt:  time.Now(),
		}
		blockId, err = storage.AddBlock(ctx, block, dbTx)
		if err != nil {
			return err
		}

		// Add claim
		claim := &etherman.Claim{
			Index:              depositCnt,
			OriginalNetwork:    0,
			Amount:             big.NewInt(1),
			DestinationAddress: destAddr,
			BlockID:            blockId,
			BlockNumber:        blockNumber,
			NetworkID:          1,
			Time:               time.Now(),
		}
		err = storage.AddClaim(ctx, claim, dbTx)
		if err != nil {
			return err
		}
	}

	// Add more block
	for blockNumber < blockCount {
		blockNumber++
		if blockNumber%logBatch == 0 {
			log.Infof("Adding block #%d", blockNumber)
		}
		block := &etherman.Block{
			BlockNumber: blockNumber,
			NetworkID:   1,
			BlockHash:   getHashFromInt(blockNumber),
			ReceivedAt:  time.Now(),
		}
		_, err := storage.AddBlock(ctx, block, dbTx)
		if err != nil {
			return err
		}
	}

	return nil
}

func runTest(ctx context.Context, storage *pgstorage.PostgresStorage, dbTx pgx.Tx) {
	wrapper := func(name string, fn func() error) {
		sumDuration := time.Duration(0)
		maxDuration := time.Duration(0)
		log.Debugf("[%v] Start running...", name)
		for i := 0; i < testIteration; i++ {
			startTime := time.Now()
			err := fn()
			if err != nil {
				log.Errorf("[%v] Iteration #%v error %v", name, i, err)
			}
			dur := time.Now().Sub(startTime)
			sumDuration += dur
			if dur > maxDuration {
				maxDuration = dur
			}
		}
		avg := sumDuration / testIteration
		log.Infof("[%v] Finished. Average running time: %v. Max running time: %v.", name, avg.String(), maxDuration.String())
	}

	// GetDeposits
	wrapper("GetDeposits", func() error {
		limit := 100
		offset := rand.Int() % (transactionCount / userCount)
		log.Debugf("random offset: %v", offset)
		_, err := storage.GetDeposits(ctx, getRandomAddress().Hex(), uint(limit), uint(offset), dbTx)
		return err
	})

	// GetClaims
	wrapper("GetClaims", func() error {
		limit := 100
		offset := rand.Int() % (transactionCount / userCount)
		log.Debugf("random offset: %v", offset)
		_, err := storage.GetClaims(ctx, getRandomAddress().Hex(), uint(limit), uint(offset), dbTx)
		return err
	})

	// GetPendingTransactions
	wrapper("GetPendingTransactions", func() error {
		limit := 100
		_, err := storage.GetPendingTransactions(ctx, getRandomAddress().Hex(), uint(limit), 0, dbTx)
		return err
	})

	// GetDeposit
	wrapper("GetDeposit", func() error {
		id := rand.Int()%transactionCount + 1
		_, err := storage.GetDeposit(ctx, uint(id), 0, dbTx)
		return err
	})

	// GetClaim
	wrapper("GetClaim", func() error {
		id := rand.Int()%claimCount + 1
		_, err := storage.GetClaim(ctx, uint(id), 1, dbTx)
		return err
	})
}

func getRandomAddress() common.Address {
	x := rand.Int()%userCount + 1
	s := fmt.Sprintf("%020x", x)
	return common.BytesToAddress([]byte(s))
}

func getHashFromInt(x uint64) common.Hash {
	s := fmt.Sprintf("%032x", x)
	return common.BytesToHash([]byte(s))
}
