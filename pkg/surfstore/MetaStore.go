package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"fmt"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddrs []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	if v, exists := m.FileMetaMap[fileMetaData.Filename]; exists{
		if fileMetaData.Version - v.Version == 1{
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
			return &Version{Version: fileMetaData.Version}, nil
		} else{
			return &Version{Version: -1}, fmt.Errorf("version for storing is not correct")
		}
	} else{
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	}
}

// func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
// 	// may add more checks for this function
// 	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
// }

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := make(map[string]*BlockHashes)
	for _, v := range blockHashesIn.Hashes {
		responsibleSer := m.ConsistentHashRing.GetResponsibleServer(v)
		if _, exists := blockStoreMap[responsibleSer]; exists{
			hashes := blockStoreMap[responsibleSer].Hashes
			hashes = append(hashes, v)
			blockStoreMap[responsibleSer] = &BlockHashes{Hashes: hashes}
		} else {
			hashes := make([]string, 0)
			hashes = append(hashes, v)
			blockStoreMap[responsibleSer] = &BlockHashes{Hashes: hashes}
		}
	}
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddrs: blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
