package rootmulti_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"testing"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	"cosmossdk.io/store/snapshots"
	snapshottypes "cosmossdk.io/store/snapshots/types"
	"cosmossdk.io/store/types"
)

type restoreBackendCase struct {
	name    string
	backend dbm.BackendType
	profile string
}

func newVersionedMultiStoreWithGeneratedData(db dbm.DB, stores int, storeKeys int, versions int, valueSize int) *rootmulti.Store {
	multiStore := rootmulti.NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics())
	r := rand.New(rand.NewSource(49872768940))

	keys := make([]*types.KVStoreKey, 0, stores)
	storeKeySet := make(map[string][][]byte, stores)
	for i := 0; i < stores; i++ {
		key := types.NewKVStoreKey(fmt.Sprintf("store%02d", i))
		multiStore.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
		keys = append(keys, key)
		storeKeySet[key.Name()] = make([][]byte, 0, storeKeys)
	}
	if err := multiStore.LoadLatestVersion(); err != nil {
		panic(err)
	}

	for _, key := range keys {
		store := multiStore.GetCommitKVStore(key).(types.KVStore)
		for i := 0; i < storeKeys; i++ {
			k := make([]byte, 8)
			v := make([]byte, valueSize)
			binary.BigEndian.PutUint64(k, uint64(i))
			if _, err := r.Read(v); err != nil {
				panic(err)
			}
			store.Set(k, v)
			storeKeySet[key.Name()] = append(storeKeySet[key.Name()], append([]byte(nil), k...))
		}
	}
	multiStore.Commit()

	if versions < 2 {
		versions = 2
	}
	for ver := 2; ver <= versions; ver++ {
		for _, key := range keys {
			store := multiStore.GetCommitKVStore(key).(types.KVStore)
			allKeys := storeKeySet[key.Name()]
			updates := len(allKeys) / 8
			if updates < 1 {
				updates = 1
			}
			for i := 0; i < updates; i++ {
				pick := allKeys[r.Intn(len(allKeys))]
				v := make([]byte, valueSize)
				if _, err := r.Read(v); err != nil {
					panic(err)
				}
				store.Set(pick, v)
			}
		}
		multiStore.Commit()
	}

	if err := multiStore.LoadLatestVersion(); err != nil {
		panic(err)
	}
	return multiStore
}

func TestRestoreSequenceBackendParity_PostRestoreCommitAndReload(t *testing.T) {
	cases := []restoreBackendCase{
		{name: "goleveldb", backend: dbm.GoLevelDBBackend},
		{name: "treedb_fast", backend: dbm.BackendType("treedb"), profile: "fast"},
		{name: "treedb_wal_on_fast", backend: dbm.BackendType("treedb"), profile: "wal_on_fast"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.backend == dbm.BackendType("treedb") {
				t.Setenv("TREEDB_OPEN_PROFILE", tc.profile)
				t.Setenv("TREEDB_FORCE_CHECKPOINT_ON_WRITE", "0")
			}

			source := newVersionedMultiStoreWithGeneratedData(dbm.NewMemDB(), 12, 4000, 8, 2048)
			version := uint64(source.LastCommitID().Version)

			targetDir := t.TempDir()
			targetDB, err := dbm.NewDB("application", tc.backend, targetDir)
			require.NoError(t, err)
			defer func() {
				if targetDB != nil {
					require.NoError(t, targetDB.Close())
				}
			}()

			target := rootmulti.NewStore(targetDB, log.NewNopLogger(), metrics.NewNoOpMetrics())
			storeKeys := source.StoreKeysByName()
			storeNames := make([]string, 0, len(storeKeys))
			for name := range storeKeys {
				storeNames = append(storeNames, name)
			}
			sort.Strings(storeNames)
			for _, name := range storeNames {
				target.MountStoreWithDB(storeKeys[name], types.StoreTypeIAVL, nil)
			}
			require.NoError(t, target.LoadLatestVersion())

			chunks := make(chan io.ReadCloser, 128)
			go func() {
				streamWriter := snapshots.NewStreamWriter(chunks)
				defer streamWriter.Close()
				require.NotNil(t, streamWriter)
				require.NoError(t, source.Snapshot(version, streamWriter))
			}()
			streamReader, err := snapshots.NewStreamReader(chunks)
			require.NoError(t, err)
			_, err = target.Restore(version, snapshottypes.CurrentFormat, streamReader)
			require.NoError(t, err)

			probes := make(map[string][]byte, len(storeNames))
			for _, name := range storeNames {
				sourceStore := source.GetStoreByName(name).(types.CommitKVStore)
				iter := sourceStore.Iterator(nil, nil)
				require.True(t, iter.Valid(), "source store %s should not be empty", name)
				probeKey := append([]byte(nil), iter.Key()...)
				iter.Close()
				probes[name] = probeKey
				expected := sourceStore.Get(probeKey)
				got := target.GetStoreByName(name).(types.CommitKVStore).Get(probeKey)
				require.NotNil(t, got, "restored target missing logical key for store %s key=%x", name, probeKey)
				require.True(t, bytes.Equal(expected, got), "restored target mismatched logical key for store %s key=%x", name, probeKey)
			}

			for idx, name := range storeNames {
				store := target.GetStoreByName(name).(types.KVStore)
				newKey := []byte(fmt.Sprintf("post/%02d", idx))
				newValue := []byte(fmt.Sprintf("value/%02d", idx))
				store.Set(newKey, newValue)
			}
			commitID := target.Commit()
			require.EqualValues(t, version+1, commitID.Version)

			for idx, name := range storeNames {
				store := target.GetStoreByName(name).(types.CommitKVStore)
				probeKey := probes[name]
				expected := source.GetStoreByName(name).(types.CommitKVStore).Get(probeKey)
				got := store.Get(probeKey)
				require.NotNil(t, got, "post-commit target missing historical key for store %s key=%x", name, probeKey)
				require.True(t, bytes.Equal(expected, got), "post-commit target mismatched historical key for store %s key=%x", name, probeKey)
				newKey := []byte(fmt.Sprintf("post/%02d", idx))
				require.Equal(t, []byte(fmt.Sprintf("value/%02d", idx)), store.Get(newKey), "post-commit target missing new key for store %s", name)
			}

			require.NoError(t, targetDB.Close())
			targetDB = nil
			reopenedDB, err := dbm.NewDB("application", tc.backend, targetDir)
			require.NoError(t, err)
			defer func() {
				if reopenedDB != nil {
					require.NoError(t, reopenedDB.Close())
				}
			}()

			reopened := rootmulti.NewStore(reopenedDB, log.NewNopLogger(), metrics.NewNoOpMetrics())
			for _, name := range storeNames {
				reopened.MountStoreWithDB(storeKeys[name], types.StoreTypeIAVL, nil)
			}
			require.NoError(t, reopened.LoadLatestVersion())
			require.EqualValues(t, version+1, reopened.LastCommitID().Version)
			for idx, name := range storeNames {
				store := reopened.GetStoreByName(name).(types.CommitKVStore)
				probeKey := probes[name]
				expected := source.GetStoreByName(name).(types.CommitKVStore).Get(probeKey)
				got := store.Get(probeKey)
				require.NotNil(t, got, "reopened target missing historical key for store %s key=%x", name, probeKey)
				require.True(t, bytes.Equal(expected, got), "reopened target mismatched historical key for store %s key=%x", name, probeKey)
				newKey := []byte(fmt.Sprintf("post/%02d", idx))
				require.Equal(t, []byte(fmt.Sprintf("value/%02d", idx)), store.Get(newKey), "reopened target missing new key for store %s", name)
			}
		})
	}
}
